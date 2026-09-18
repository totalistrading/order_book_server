# Trigger repricing regression

Derived from Mainnet block 1151630595, 2026-09-18T02:36:17.793733913Z. A UNI position stop-market order had status limitPx 7.1973 and size 0; the paired new book diff placed it at price 7.917 and size 4.6. The authoritative snapshot reload diagnostic and later filled status confirmed resting price 7.917, size before fill 4.6, and activated timestamp 1789698977793.

The fixture replaces the user with the zero address, order ID with 1, removes the client ID, and renumbers heights to 100/101. The local arrival time is synthetic. It preserves the observed price, size, source time and trigger metadata. The expected snapshot is reconstructed from these observed canonical fields; it is not a raw full-node snapshot dump.

Before the fix, the test fails strict snapshot equality because reconstruction retains price 7.1973. After the fix, full order equality and the L2 price/size pass. Validation and gap fences are unchanged.
