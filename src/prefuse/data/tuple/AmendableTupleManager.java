package prefuse.data.tuple;

import prefuse.data.Graph;
import prefuse.data.Table;
import prefuse.data.Tuple;

/**
 * Manager for {@link Tuple}s that allows a pre-exiting tuple to be added.
 * 
 * <p>
 * Preconditions are that {@link Tuple} must be invalid and supported by the
 * tuple type. Consequently, the {@link Tuple} cannot hold any data before it is
 * added.
 * 
 * <p>
 * This tuple manager keeps the same tuple instance from the very beginning.
 * Especially, the tuples seen by a {@link TupleSetListener} will stay valid.
 * 
 * @author Rind
 */
public class AmendableTupleManager extends TupleManager {

    /**
     * temporarily store an existing tuple that should be added. <tt>null</tt>
     * if the next tuple will be created by the default approach.
     */
    private TableTuple amendTuple = null;

    /**
     * object to provide intrinsic lock of this instance for a single thread.
     */
    private Object lock = new Object();

    /**
     * Create a new TupleManager for the given Table.
     * 
     * @param t
     *            the data Table to generate Tuples for
     * @param g
     *            keep track of a Graph (if applicable)
     * @param tupleType
     *            the type of created tuples
     */
    public AmendableTupleManager(Table t, Graph g,
            @SuppressWarnings("rawtypes") Class tupleType) {
        super(t, g, tupleType);
    }

    /**
     * adds the tuple to the tuple manager and a row to the underlying table.
     * 
     * @param tuple
     *            a tuple instance to be added
     * @return the row index
     * @throws IllegalArgumentException
     *             if the tuple is already valid or has an incompatible type
     */
    public int amendTuple(TableTuple tuple) {
        int row;
        // Tuple controlTuple;
        // check Tuple instanceof Class
        if (!super.m_tupleType.isInstance(tuple)) {
            throw new IllegalArgumentException(
                    "The amended tuple has an incomaptible type. "
                            + "Must be subclass of "
                            + super.m_tupleType.getName());
        }
        // check Invalid(Tuple)
        if (tuple.isValid()) {
            throw new IllegalArgumentException(
                    "The amended tuple is already valid");
        }
        // if (this.amendTuple != null)
        // throw new RuntimeException("not null");

        // synchronize threads
        synchronized (lock) {
            // remember tuple
            this.amendTuple = tuple;
            // add row to underlying table -> will call newTuple()
            row = m_table.addRow();
            // will call newTuple(), if this is not set as manager of the table
            this.getTuple(row);
            // reset remembered tuple
            this.amendTuple = null;
        }
        // check if it worked as expected (e.g. multi thread issue)
        // if (row != tuple.getRow())
        // throw new RuntimeException("Amended tuple not identical: " + row +
        // " " + tuple.getRow());
        // if (controlTuple != tuple)
        // throw new RuntimeException("Amended tuple not identical: " +
        // controlTuple + " " + tuple);

        return row;
    }

    /*
     * (non-Javadoc)
     * 
     * @see prefuse.data.tuple.TupleManager#newTuple(int)
     */
    @Override
    protected final TableTuple newTuple(int row) {
        synchronized (lock) {
            if (this.amendTuple != null) {
                amendTuple.init(m_table, m_graph, row);
                return amendTuple;
            } else {
                return reallyNewTuple(row);
            }
        }
    }

    /**
     * Instantiate a new {@link Tuple} instance if no existing instance has been
     * provided.
     * 
     * @param row
     *            the row index of the tuple
     * @return the newly created Tuple
     */
    protected TableTuple reallyNewTuple(int row) {
        return super.newTuple(row);
    }
}
