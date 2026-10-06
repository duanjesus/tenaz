package dev.tenaz.journal;

class InMemoryJournalTest extends JournalContractTest {

    @Override
    protected Journal createJournal() {
        return new InMemoryJournal();
    }
}
