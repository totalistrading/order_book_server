//! Bounded filesystem work. A notification is a dirty-path hint, not a record.
use std::{
    collections::{HashMap, VecDeque},
    fs::{File, Metadata},
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

/// Coalesces repeated notifications without retaining an unbounded event queue.
/// Overflow is explicit: the caller must fence and resynchronize before publishing.
pub(super) struct DirtyFiles {
    paths: VecDeque<PathBuf>,
    created: HashMap<PathBuf, bool>,
    limit: usize,
    overflow: bool,
}

impl DirtyFiles {
    pub(super) fn new(limit: usize) -> Self {
        Self { paths: VecDeque::new(), created: HashMap::new(), limit, overflow: false }
    }

    pub(super) fn push(&mut self, path: PathBuf, created: bool) {
        if let Some(existing) = self.created.get_mut(&path) {
            *existing |= created;
        } else if self.paths.len() >= self.limit {
            self.overflow = true;
        } else {
            self.created.insert(path.clone(), created);
            self.paths.push_back(path);
        }
    }

    pub(super) fn pop(&mut self) -> Option<(PathBuf, bool)> {
        self.paths.pop_front().map(|path| {
            let created = self.created.remove(&path).unwrap_or(false);
            (path, created)
        })
    }

    pub(super) fn len(&self) -> usize {
        self.paths.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.paths.is_empty()
    }

    pub(super) fn mark_overflow(&mut self) {
        self.overflow = true;
    }

    pub(super) fn take_overflow(&mut self) -> bool {
        std::mem::take(&mut self.overflow)
    }
}

/// A cursor keeps its byte offset and partial record between bounded reads.
/// No UTF-8 parsing occurs until a complete newline-terminated record is available.
pub(super) struct FileCursor {
    file: File,
    offset: u64,
    partial: Vec<u8>,
    max_record_bytes: usize,
    partial_record_bytes: usize,
    partial_since: Option<Instant>,
    max_partial_age: Duration,
}

impl FileCursor {
    pub(super) fn open(path: &Path, from_end: bool, max_record_bytes: usize) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let offset = if from_end { file.seek(SeekFrom::End(0))? } else { 0 };
        Ok(Self {
            file,
            offset,
            partial: Vec::new(),
            max_record_bytes,
            partial_record_bytes: 0,
            partial_since: None,
            max_partial_age: Duration::from_secs(120),
        })
    }

    pub(super) fn backlog_bytes(&self) -> u64 {
        self.file.metadata().map_or(0, |metadata| metadata.len().saturating_sub(self.offset))
            + self.partial.len() as u64
    }

    pub(super) fn drained(&self) -> bool {
        self.partial.is_empty() && self.file.metadata().is_ok_and(|metadata| metadata.len() == self.offset)
    }

    pub(super) fn with_max_age(mut self, age: Duration) -> Self {
        self.max_partial_age = age;
        self
    }

    fn partial_expired(&self) -> bool {
        self.partial_since.is_some_and(|at| at.elapsed() > self.max_partial_age)
    }

    fn verify_file(&self, path: &Path) -> io::Result<Metadata> {
        let metadata = self.file.metadata()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let current = path.metadata()?;
            if (metadata.dev(), metadata.ino()) != (current.dev(), current.ino()) {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "source file replaced; resnapshot required"));
            }
        }
        if metadata.len() < self.offset {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "source file truncated; resnapshot required"));
        }
        Ok(metadata)
    }

    pub(super) fn needs_attention(&self, path: &Path) -> bool {
        self.partial_expired() || self.verify_file(path).map_or(true, |metadata| metadata.len() > self.offset)
    }

    pub(super) fn read_turn(&mut self, path: &Path, budget: usize) -> io::Result<(String, bool, usize)> {
        let _metadata = self.verify_file(path)?;
        if self.partial_expired() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "partial source record exceeded age limit; resnapshot required",
            ));
        }
        let mut bytes = vec![0; budget];
        let read = self.file.read(&mut bytes)?;
        self.offset += read as u64;
        // Scan only newly read bytes: large partial records must not be rescanned
        // from their beginning on every turn.
        let prior_bytes = self.partial.len();
        let mut complete_end = None;
        for (index, byte) in bytes[..read].iter().enumerate() {
            self.partial_record_bytes += 1;
            if self.partial_record_bytes > self.max_record_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "source record exceeds byte limit; resnapshot required",
                ));
            }
            if *byte == b'\n' {
                self.partial_record_bytes = 0;
                complete_end = Some(prior_bytes + index + 1);
            }
        }
        self.partial.extend_from_slice(&bytes[..read]);
        let data = if let Some(end) = complete_end {
            let remaining = self.partial.split_off(end);
            let complete = std::mem::replace(&mut self.partial, remaining);
            String::from_utf8(complete).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?
        } else {
            String::new()
        };
        if self.partial.is_empty() {
            self.partial_since = None;
        } else if complete_end.is_some() || self.partial_since.is_none() {
            self.partial_since = Some(Instant::now());
        }
        Ok((data, read > 0, read))
    }
}

/// Prefer a drained old file from the rotating stream; never evict unread work.
pub(super) fn evict_drained_cursor(cursors: &mut HashMap<PathBuf, FileCursor>, root: &Path) -> bool {
    let oldest = cursors
        .iter()
        .filter(|(path, cursor)| cursor.drained() && !cursor.needs_attention(path))
        .min_by_key(|(path, _)| {
            (!path.starts_with(root), path.metadata().and_then(|metadata| metadata.modified()).ok())
        })
        .map(|(path, _)| path.clone());
    if let Some(path) = oldest {
        cursors.remove(&path);
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    struct Fixture(PathBuf);
    impl Fixture {
        fn new(data: &[u8]) -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "tt1506-cursor-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::write(&path, data).unwrap();
            Self(path)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _unused = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    fn burst_notifications_coalesce_and_overflow_is_explicit() {
        let mut dirty = DirtyFiles::new(2);
        for _ in 0..100_000 {
            dirty.push("a".into(), false);
        }
        dirty.push("a".into(), true);
        dirty.push("b".into(), false);
        dirty.push("c".into(), false);
        assert!(dirty.take_overflow());
        assert!(!dirty.take_overflow());
        assert_eq!(dirty.pop(), Some(("a".into(), true)));
        // Requeued busy files go behind existing work.
        dirty.push("a".into(), false);
        assert_eq!(dirty.pop(), Some(("b".into(), false)));
        assert_eq!(dirty.pop(), Some(("a".into(), false)));
        assert!(dirty.pop().is_none());
    }

    #[test]
    fn partial_utf8_records_survive_bounded_reads_and_append() {
        use std::io::Write;
        let fixture = Fixture::new("first\né".as_bytes());
        let mut cursor = FileCursor::open(&fixture.0, false, 100).unwrap();
        let mut received = String::new();
        for _ in 0..5 {
            received += &cursor.read_turn(&fixture.0, 2).unwrap().0;
        }
        assert_eq!(received, "first\n");
        std::fs::OpenOptions::new().append(true).open(&fixture.0).unwrap().write_all(b"nd\n").unwrap();
        for _ in 0..3 {
            received += &cursor.read_turn(&fixture.0, 2).unwrap().0;
        }
        assert_eq!(received, "first\nénd\n");
        assert!(cursor.partial.is_empty());
    }

    #[test]
    fn record_limit_and_truncation_are_explicit_gaps() {
        let fixture = Fixture::new(b"123456789");
        let mut cursor = FileCursor::open(&fixture.0, false, 8).unwrap();
        assert!(cursor.read_turn(&fixture.0, 8).is_ok());
        assert!(cursor.read_turn(&fixture.0, 8).is_err());
        let mut cursor = FileCursor::open(&fixture.0, true, 100).unwrap();
        std::fs::write(&fixture.0, b"x\n").unwrap();
        assert!(cursor.read_turn(&fixture.0, 8).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn replacement_at_same_path_requires_resnapshot() {
        let fixture = Fixture::new(b"old\n");
        let mut cursor = FileCursor::open(&fixture.0, true, 100).unwrap();
        let replacement = Fixture::new(b"new\n");
        std::fs::rename(&replacement.0, &fixture.0).unwrap();
        assert!(cursor.needs_attention(&fixture.0));
        assert!(cursor.read_turn(&fixture.0, 8).is_err());
    }
    #[test]
    fn oversized_interval_is_skipped_once_then_new_records_can_progress() {
        use std::io::Write;
        let fixture = Fixture::new(b"oversized-record\n");
        let mut cursor = FileCursor::open(&fixture.0, false, 8).unwrap();
        assert!(cursor.read_turn(&fixture.0, 16).is_err());
        let mut cursor = FileCursor::open(&fixture.0, true, 8).unwrap();
        std::fs::OpenOptions::new().append(true).open(&fixture.0).unwrap().write_all(b"ok\n").unwrap();
        assert_eq!(cursor.read_turn(&fixture.0, 4).unwrap().0, "ok\n");
        assert!(cursor.drained());
    }

    #[test]
    fn rotation_keeps_the_old_partial_record_and_new_file_offset_independent() {
        use std::io::Write;
        let old = Fixture::new(b"old");
        let new = Fixture::new(b"new\n");
        let mut old_cursor = FileCursor::open(&old.0, false, 100).unwrap();
        let mut new_cursor = FileCursor::open(&new.0, false, 100).unwrap();
        assert_eq!(old_cursor.read_turn(&old.0, 4).unwrap().0, "");
        assert_eq!(new_cursor.read_turn(&new.0, 4).unwrap().0, "new\n");
        std::fs::OpenOptions::new().append(true).open(&old.0).unwrap().write_all(b"\n").unwrap();
        assert_eq!(old_cursor.read_turn(&old.0, 4).unwrap().0, "old\n");
        assert!(old_cursor.drained());
        assert!(new_cursor.drained());
    }
    #[test]
    fn quiet_cursors_do_not_fill_dirty_queue_and_append_is_detected() {
        use std::io::Write;
        let fixture = Fixture::new(b"done\n");
        let cursor = FileCursor::open(&fixture.0, true, 100).unwrap();
        let mut queue = DirtyFiles::new(1);
        for _ in 0..100 {
            if cursor.needs_attention(&fixture.0) {
                queue.push(fixture.0.clone(), false);
            }
        }
        queue.push("new-file".into(), true);
        assert!(!queue.take_overflow());
        assert_eq!(queue.len(), 1);
        std::fs::OpenOptions::new().append(true).open(&fixture.0).unwrap().write_all(b"next\n").unwrap();
        assert!(cursor.needs_attention(&fixture.0));
    }

    #[test]
    fn partial_records_wait_for_bytes_but_expire_without_new_notifications() {
        let fixture = Fixture::new(b"partial");
        let mut cursor = FileCursor::open(&fixture.0, false, 100).unwrap();
        cursor.read_turn(&fixture.0, 16).unwrap();
        assert!(!cursor.needs_attention(&fixture.0));
        cursor.partial_since = Some(Instant::now() - Duration::from_secs(121));
        assert!(cursor.needs_attention(&fixture.0));
        assert!(cursor.read_turn(&fixture.0, 16).is_err());
    }

    #[test]
    fn cursor_capacity_reclaims_only_quiet_drained_files() {
        let quiet = Fixture::new(b"done\n");
        let unread = Fixture::new(b"pending\n");
        let mut cursors = HashMap::from([
            (quiet.0.clone(), FileCursor::open(&quiet.0, true, 100).unwrap()),
            (unread.0.clone(), FileCursor::open(&unread.0, false, 100).unwrap()),
        ]);
        assert!(evict_drained_cursor(&mut cursors, &std::env::temp_dir()));
        assert!(!cursors.contains_key(&quiet.0));
        assert!(cursors.contains_key(&unread.0));
        assert!(!evict_drained_cursor(&mut cursors, &std::env::temp_dir()));
    }
}
