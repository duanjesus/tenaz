package dev.tenaz.journal;

import dev.tenaz.PostgresTestSupport;
import org.junit.jupiter.api.AfterEach;

class PostgresJournalTest extends JournalContractTest {

    @Override
    protected Journal createJournal() {
        return PostgresTestSupport.freshJournal();
    }

    @AfterEach
    void closeJournal() {
        ((PostgresJournal) journal).close();
    }
}
