#include <gtest/gtest.h>
#include <cstdlib>

int main(int argc, char *argv []){
    srand(time(NULL));

	std::cout << "Running main() from dbzero_tests.cpp\n";
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
