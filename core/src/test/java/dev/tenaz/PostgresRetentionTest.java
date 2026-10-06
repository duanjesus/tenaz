package dev.tenaz;

import dev.tenaz.journal.Journal;
import dev.tenaz.journal.PostgresJournal;
import org.junit.jupiter.api.AfterEach;

class PostgresRetentionTest extends RetentionTest {

    @Override
    protected Journal createJournal() {
        return PostgresTestSupport.freshJournal();
    }

    @AfterEach
    void closeJournal() {
        ((PostgresJournal) journal).close();
    }
}
