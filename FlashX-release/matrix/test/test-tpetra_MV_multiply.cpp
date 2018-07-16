#include "FG2Tpetra.h"

using namespace fm;

typedef Tpetra::DefaultPlatform::DefaultPlatformType Platform;

RCP<const Teuchos::Comm<int> > comm;
RCP<map_type> Map;

size_t long_dim = 60 * 1024 * 1024;
size_t num_repeats = 1;

void test_gemm(size_t block_size, size_t num_blocks)
{
	const int myRank = comm->getRank ();
	RCP<MV> A = rcp(new MV(Map, block_size * num_blocks));
	RCP<MV> res = rcp(new MV(Map, block_size));
	A->randomize ();

	Teuchos::SerialDenseMatrix<size_t, double> B(block_size * num_blocks, block_size, false);
	B.random();
	Teuchos::SerialComm<int> serialComm;
	map_type LocalMap (B.numRows (), A->getMap ()->getIndexBase (),
			Teuchos::rcpFromRef<const Teuchos::Comm<int> > (serialComm),
			Tpetra::LocallyReplicated, A->getMap()->getNode ());
	// encapsulate Teuchos::SerialDenseMatrix data in ArrayView
	Teuchos::ArrayView<const double> Bvalues (B.values (), B.stride () * B.numCols ());
	// create locally replicated MultiVector with a copy of this data
	MV B_mv (Teuchos::rcpFromRef (LocalMap), Bvalues, B.stride (), B.numCols ());

	for (size_t i = 0; i < num_repeats; i++) {
		struct timeval start, end;
		gettimeofday(&start, NULL);
		res->multiply(Teuchos::NO_TRANS, Teuchos::NO_TRANS, 1, *A, B_mv, 0);
		comm->barrier();
		gettimeofday(&end, NULL);
		if (myRank == 0)
			printf("GEMM %d takes %.3f seconds\n", myRank, time_diff(start, end));
	}
}

void test_MvTransMv(size_t block_size, size_t num_blocks)
{
	const int myRank = comm->getRank ();
	RCP<MV> A = rcp(new MV(Map, block_size * num_blocks));
	RCP<MV> B = rcp(new MV(Map, block_size));
	A->randomize ();
	B->randomize ();

	RCP<Teuchos::Comm<int> > serialComm (new Teuchos::SerialComm<int> ());
	RCP<const map_type> LocalMap = rcp(new map_type(block_size * num_blocks,
				0, serialComm, Tpetra::LocallyReplicated, A->getMap()->getNode()));
	MV res(LocalMap, block_size, true);

	for (size_t i = 0; i < num_repeats; i++) {
		struct timeval start, end;
		gettimeofday(&start, NULL);
		res.multiply(Teuchos::CONJ_TRANS, Teuchos::NO_TRANS, 1, *A, *B, 0);
		comm->barrier();
		gettimeofday(&end, NULL);
		if (myRank == 0)
			printf("MvTransMv %d takes %.3f seconds\n", myRank, time_diff(start, end));
	}
}

int main(int argc, char *argv[])
{
	Teuchos::GlobalMPISession mpiSession (&argc, &argv);
	printf("MPI initialized: %d, #procs: %d, rank: %d\n",
			Teuchos::GlobalMPISession::mpiIsInitialized(),
			Teuchos::GlobalMPISession::getNProc(),
			Teuchos::GlobalMPISession::getRank());

	Platform &platform = Tpetra::DefaultPlatform::getDefaultPlatform();
	comm = platform.getComm();
	Map = rcp (new map_type(long_dim, 0, comm));

	for (size_t num_blocks = 1; num_blocks <= 128; num_blocks *= 2) {
		if (comm->getRank () == 0)
			printf("#blocks: %ld\n", num_blocks);
		test_gemm(4, num_blocks);
	}

	for (size_t num_blocks = 1; num_blocks <= 128; num_blocks *= 2) {
		if (comm->getRank () == 0)
			printf("#blocks: %ld\n", num_blocks);
		test_MvTransMv(4, num_blocks);
	}
}
