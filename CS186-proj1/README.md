Proj2
Implementaions:

Filter and Join Operaters:
	follow Javadoc and Project and OrderBy.
IntegerAggregator and AtringAggregator: 
	Computes an aggregate over a particular field across multiple groups in a sequence 	of input tuples.
	Use integer Division for computing the avg, (SimpleDB only supports integers).
	StringAggregator only needs to support the COUNT aggregate (other operations do not make sense for strings.)
Aggregate operator:
	Aggregates implement DbIterator interface (so that they can be placed in SimpleDB query plans)
	Output is 