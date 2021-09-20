//compile with: g++ -std=c++11 -D_NDEBUG -O3 External_Sort.cpp

#include <fstream>
#include <iostream>
#include <vector>
#include <queue>
#include <thread>
#include <string>
#include <cstdint>
#include <cstdlib>
#include <random>
#include <limits>


using namespace std;


// the amount of cores on the local machine
static const auto processor_count = std::thread::hardware_concurrency();
//static const auto processor_count = 1;

// vector to store the names of tempTemp files
static std::vector<std::string> tempTempNameVector;



std::ifstream openInputFile(std::string fileName, std::ios::openmode mode)
{
	std::ifstream stream(fileName, mode);
	if (!stream.is_open()) {
		std::cerr << "Could not open the file - '" << fileName << "'" << std::endl;
		exit(1);
	}
    return stream;
}

std::ofstream openOutputFile(std::string fileName, std::ios::openmode mode)
{
	std::ofstream stream(fileName, mode);
	if (!stream.is_open()) {
		std::cerr << "Could not open the file - '" << fileName << "'" << std::endl;
		exit(1);
	}
    return stream;
}

void openInputStream(std::ifstream& stream, std::string fileName, std::ios::openmode mode)
{
	stream.open(fileName, mode);
	if (!stream.is_open()) {
		std::cerr << "Could not open the file - '" << fileName << "'" << std::endl;
		exit(1);
	}
}


std::string split(std::string input_file_name, int threadNumber, long long fileStartLocation, long long fileEndLocation){

	std::ifstream in = openInputFile(input_file_name, std::ios::binary);

	// name the temp file the same as the thread number and open it
	std::string fileNameTemp = std::to_string(threadNumber);
	std::ofstream outTemp = openOutputFile(fileNameTemp, std::ios::binary);

	in.seekg(fileStartLocation, std::ios::beg);

	// count the total bytes, starting from the start location in the file
	long long totalBytes = fileStartLocation;

	// read the input file, 8 bytes at a time until the end location and output to a temp file
	constexpr size_t bufferSize = 8;
	char buffer[bufferSize];

	while ((in.read(buffer, bufferSize) || in.gcount()) && totalBytes < fileEndLocation)
	{
		totalBytes += in.gcount();
		std::string line(buffer, in.gcount());

		// remove commas if they exist
//		line.erase(std::remove(line.begin(), line.end(), ','), line.end());

		// read the correct amount of characters from the last line
		if (totalBytes > fileEndLocation){
			line = line.substr(0, (fileEndLocation - fileStartLocation) % bufferSize );
		}

		outTemp << line;
	}

	in.seekg(0, std::ios::beg);


	outTemp.close();
	in.close();

	return fileNameTemp;
}

int compVar(const void *one, const void *two)
{
	std::uint32_t a = *((std::uint32_t*)one);
	std::uint32_t b = *((std::uint32_t*)two);

    if (a < b){
       return -1;
    }

    if (a == b){
       return 0;
    }

    return 1;
}

void removeFile(const char* fileName){
	int status = std::remove(fileName);
	std::cout << "Removed file: " << fileName << std::endl;
	if(status != 0){
		std::cerr << "Error removing file '" << fileName << "'" << std::endl;
	}
}

void sortWrite(std::string input_file_name){

	std::ifstream inTemp = openInputFile(input_file_name, std::ios::binary);

	int totalMemoryBytes = 128 * 1024 * 1024;
	int i = 0;
	bool more_input = true;
	int next_tempTemp_file = 0;

	// the loop runs until there is no more data in the input file
	while (more_input) {

		// create a vector and dynamically allocate (128 / processor_count) MB of memory
	    // to avoid reallocation of the vector
	    std::vector<std::uint32_t> vect;
	    vect.reserve(totalMemoryBytes / processor_count / sizeof(std::uint32_t));

	    // for-loop to process (totalMemoryBytes / processor_count / sizeof(std::uint32_t) ) amount of elements
	    // and store them in the vector;
	    // If the amount of integers in the vector has not reached (totalMemoryBytes / processor_count / sizeof(std::uint32_t) ),
	    // then we exit the loop
	    std::uint32_t get;
	    for (i = 0; i < totalMemoryBytes / processor_count / sizeof(std::uint32_t); i++)
	    {
	    	if (inTemp >> get){
	        	vect.push_back(get);
	    	} else {
	        	more_input = false;
	        	break;
	        }
	    }

	    // if there are any integers to be processed in vect,
	    // sort the vector and output to tempTemp file
	    if (i > 0){

//	    	std::sort(vect.begin(), vect.end());
	    	std::qsort(&vect[0], vect.size(), sizeof(std::uint32_t), compVar);

	    	// create a new tempTemp file with name "fileNameTemp.next_temp_file" and open it
	    	std::string fileNameTempTemp = input_file_name + "." + std::to_string(next_tempTemp_file);
	    	std::ofstream outTempTemp = openOutputFile(fileNameTempTemp, std::ios::binary);

	    	// add tempTemp file names to the tempTempNameVector to use them later, when merging
	    	tempTempNameVector.push_back(fileNameTempTemp);

	    	// write the sorted data to the "fileNameTemp.next_tempTemp_file" file
	    	for (int j = 0; j < i; j++){
	    		outTempTemp << vect[j] << " ";
	    	}

	    	outTempTemp.close();
	    	next_tempTemp_file++;
	    }
	}

	inTemp.close();

	// delete temp file
    const char *strTempFile = input_file_name.c_str();
    removeFile(strTempFile);
}

void taskSplitSortWrite(std::string input_file_name, int threadNumber, long long fileStartLocation, long long fileEndLocation)
{
	// split the original input file into temp files
	std::string fileNameTemp = split(input_file_name, threadNumber, fileStartLocation, fileEndLocation);

	// read the temp file, sort it and write the results to tempTemp files
	sortWrite(fileNameTemp);
}

void getSortedFiles(std::string input_file_name)
{
	std::ifstream in = openInputFile(input_file_name, std::ios::binary);

    in.seekg(0, std::ios::end);
    std::streamoff fileSize = in.tellg();
    in.seekg(0, std::ios::beg);

    // determine the splitting locations of the input file
    std::vector<long long> filePartitions(processor_count - 1);
    long long increment = (fileSize - 1) / processor_count;
    for (int i = 0 ; i < processor_count - 1; i++){
    	in.seekg(increment, std::ios::beg);

    	while (increment < fileSize - 1 && !isspace(in.peek())){
    		increment++;
    		in.seekg(increment, std::ios::beg);
    	}

    	// add one "space" character length at the end of each partition
    	long long lastCharLength = 1;
    	filePartitions[i] = increment + lastCharLength;

    	increment += fileSize / processor_count;
    }
    in.seekg(0, std::ios::beg);
    in.close();


    // create a vector of threads and run them;
    // each thread is responsible for reading one partition of the input file,
    // creating a temp file, then using it to create sorted tempTemp files
    std::vector<std::thread> threads(processor_count);

    if (processor_count == 1){
    	threads[0] = std::thread(taskSplitSortWrite, input_file_name, 0, 0, fileSize);
    } else if (processor_count == 2){
    	threads[0] = std::thread(taskSplitSortWrite, input_file_name, 0, 0, filePartitions[0]);
    	threads[1] = std::thread(taskSplitSortWrite, input_file_name, 1, filePartitions[0], fileSize);
    } else if (processor_count > 2) {
    	threads[0] = std::thread(taskSplitSortWrite, input_file_name, 0, 0, filePartitions[0]);
    	int i;
    	for (i = 1; i < processor_count - 1; i++){
    		threads[i] = std::thread(taskSplitSortWrite, input_file_name, i, filePartitions[i - 1], filePartitions[i]);
    	}
    	threads[i] = std::thread(taskSplitSortWrite, input_file_name, i, filePartitions[i - 1], fileSize);
    }

    for (int i = 0; i < threads.size(); i++){
    	threads[i].join();
    }

}




struct Compare
{
    // ascending order sort
    bool operator() (pair<std::uint32_t, int> p1, pair<std::uint32_t, int> p2)
    {
        return p1.first > p2.first;
    }
};

void mergeFiles(std::string input_file_name, std::string output_file_name) {

	std::ofstream outputFile = openOutputFile(output_file_name, std::ios::binary);

	std::ifstream *in = new std::ifstream[tempTempNameVector.size()];

	// if there is only one tempTemp file, simply copy it to the output file
	if (tempTempNameVector.size() == 1){

		// open the single tempTemp file
		openInputStream(in[0], tempTempNameVector[0], std::ios::binary);
		bool firstIteration = true;
		std::uint32_t value;
		while (in[0] >> value){
			if (firstIteration == true){
				outputFile << value;
				firstIteration = false;
			} else {
				outputFile << " " << value;
			}
		}

	// if there is more than one tempTemp file, perform the merge
	} else if (tempTempNameVector.size() > 1){

		// create a heap in the form of a priority queue
		// the pairs will be sorted in ascending order by the first number(uint32_t) in each pair
		std::priority_queue<pair<std::uint32_t, int>, std::vector<pair<std::uint32_t, int> >, Compare> minHeap;

		for (int i = 0; i < tempTempNameVector.size(); i++) {

			// open the existing tempTemp files
			openInputStream(in[i], tempTempNameVector[i], std::ios::binary);

			// get the first number from each file
			std::uint32_t value;
			if (in[i] >> value){
				// the first number (value) in the pair is obtained from the file
				// the second number (i) in the pair keeps track of the file from which the number was drawn
				minHeap.push(pair<std::uint32_t, int>(value, i));
			}
		}

		// perform the first pop manually, outside of the subsequent loop,
		// in order to write the starting integer to the file without spaces
		bool popCompleted = false;
		pair<std::uint32_t, int> topPair;
		if (minHeap.size() > 0){

			// obtain the minimum pair of the heap and remove it
			topPair = minHeap.top();
			minHeap.pop();

			// write the first number to the file without a preceding space
			outputFile << topPair.first;
			popCompleted = true;
		}

		while (minHeap.size() > 0) {

			if (popCompleted == false){
				// obtain the minimum pair of the heap and remove it
				topPair = minHeap.top();
				minHeap.pop();

				// write the rest of the numbers to the file with a preceding space
				outputFile << " " << topPair.first;
			}

			// read the next value from the same file that topPair was from
			// and push it onto the heap
			std::uint32_t nextValue;
			if (in[topPair.second] >> nextValue) {
				minHeap.push(pair <std::uint32_t, int>(nextValue, topPair.second));
			}
			popCompleted = false;
		}

	}

	// close tempTemp file streams
	for (int i = 0; i < tempTempNameVector.size(); i++){
		in[i].close();
	}

	delete[] in;

    outputFile.close();

    if (tempTempNameVector.size() >= 1){
    	// find the sizes of the input and output files and if the input file is larger than the output file,
    	// append the correct amount of characters to the output file
    	std::ifstream inputFile = openInputFile(input_file_name, std::ios::binary);
    	inputFile.seekg(0, std::ios::end);
    	std::streamoff fileSizeInput = inputFile.tellg();
    	inputFile.seekg(0, std::ios::beg);
    	inputFile.close();

    	std::ifstream outputFile2 = openInputFile(output_file_name, std::ios::binary);
    	outputFile2.seekg(0, std::ios::end);
    	std::streamoff fileSizeOutput = outputFile2.tellg();
    	outputFile2.seekg(0, std::ios::beg);
    	outputFile2.close();

    	int diff = fileSizeInput - fileSizeOutput;
    	std::ofstream outputFileFinal = openOutputFile(output_file_name, std::ios::app);
    	if (diff > 0){
    		for (int i = 0; i < diff; i++){
    			outputFileFinal << " ";
    		}
    	}
    	outputFileFinal.close();
    }

    // delete tempTemp files
    for(int i = 0; i < tempTempNameVector.size(); i++){
    	const char *strTempTempFile = tempTempNameVector[i].c_str();
    	removeFile(strTempTempFile);
    }
}


void generateFile(std::string inputFile, double sizeMB){
	std::ofstream out = openOutputFile(inputFile, std::ios::binary);

	std::random_device rd;     // get a random seed from the OS entropy device
	std::mt19937_64 eng(rd()); // use the 64-bit Mersenne Twister 19937 generator and seed it with entropy.

	// define the distribution, by default it goes from 0 to MAX(uint32_t)
	std::uniform_int_distribution<std::uint32_t> distr;

	// generate file containing 32-bit unsigned integers (each integer is ~10 characters long)
	for (int i = 0; i < (sizeMB * 1024 * 1024) / 10; i++){
		out << distr(eng) << " ";
	}
	out.close();
}


//void checkOrder(std::string input_file_name){
//	std::ifstream in = openInputFile(input_file_name, std::ios::binary);
//
//	in.seekg(0, std::ios::end);
//	std::streamoff fileSize = in.tellg();
//	in.seekg(0, std::ios::beg);
//
//	std::vector<std::uint32_t> vect;
//	vect.reserve(fileSize / 10);
//
//	std::uint32_t get;
//	while (in >> get){
//		vect.push_back(get);
//	}
//	in.close();
//
//
//	std::cout << "Count elements in Input file = " << vect.size() << std::endl;
//
//    std::cout << "Numbers are in order: ";
//
//    bool result = true;
//	if (vect.size() == 0){
//		result = false;
//		std::cout << result << std::endl;
//		return;
//	}
//
//	for (auto i = vect.begin() + 1; i != vect.end(); i++){
//		if (*(i - 1) > *i){
//			result =  false;
//			break;
//		}
//	}
//
//	std::cout << result << std::endl;
//}


//void matchesInputOutput(std::string input_file_name, std::string output_file_name, int inputFileSize, int outputFileSize){
//
//	std::ifstream in = openInputFile(input_file_name, std::ios::binary);
//
//	std::ifstream out = openInputFile(output_file_name, std::ios::binary);
//
//	std::cout << "Numbers are the same in both files: ";
//
//	bool result = true;
//	if (inputFileSize == 0 || outputFileSize == 0 || inputFileSize != outputFileSize){
//		result = false;
//		std::cout << result << std::endl;
//		return;
//	}
//
//	std::vector<std::uint32_t> vectInput;
//	vectInput.reserve(inputFileSize / sizeof(std::uint32_t));
//
//	std::vector<std::uint32_t> vectOutput;
//	vectOutput.reserve(outputFileSize / sizeof(std::uint32_t));
//
//	std::uint32_t getInput;
//	std::uint32_t getOutput;
//	while (in >> getInput && out >> getOutput){
//		vectInput.push_back(getInput);
//		vectOutput.push_back(getOutput);
//	}
//
//	out.close();
//	in.close();
//
////	std::sort(vectInput.begin(), vectInput.end());
//	std::qsort(&vectInput[0], vectInput.size(), sizeof(std::uint32_t), compVar);
//
//	auto i = vectInput.begin();
//	for (auto j = vectOutput.begin(); i != vectInput.end(); i++, j++){
//		if (*i != *j){
//			result = false;
//			break;
//		}
//	}
//	std::cout << result << std::endl;
//}




int main()
{
	std::string inputFile = "input.txt";
	std::string outputFile = "output";

	// generate input file of specified size in MB
	generateFile(inputFile, 500);


	// obtain temporary files and sort them
	getSortedFiles(inputFile);



	// merge the temporary files
    mergeFiles(inputFile, outputFile);


    // display input and output file sizes
    std::ifstream in = openInputFile(inputFile, std::ios::binary);
    in.seekg(0, std::ios::end);
    std::streamoff inputFileSize = in.tellg();
    in.seekg(0, std::ios::beg);
    in.close();

    in = openInputFile(outputFile, std::ios::binary);
    in.seekg(0, std::ios::end);
    std::streamoff outputFileSize = in.tellg();
    in.seekg(0, std::ios::beg);
    in.close();

    std::cout << "Input file size = " << inputFileSize << std::endl;
    std::cout << "Output file size = " << outputFileSize << std::endl;



    // check if the numbers in the output file are in sorted order
//    checkOrder(outputFile);


    // verify that the input and output files contain the same numbers
//    matchesInputOutput(inputFile, outputFile, inputFileSize, outputFileSize);


    return 0;
}
