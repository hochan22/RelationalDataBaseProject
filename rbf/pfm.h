#ifndef _pfm_h_
#define _pfm_h_

typedef unsigned PageNum;
typedef int RC;
typedef char byte;

#define PAGE_SIZE 4096
#include <string>
#include <climits>
#include <fstream>
#include <set>
using namespace std;

class FileHandle;

class PagedFileManager
{
public:
    static PagedFileManager* instance();                                  // Access to the _pf_manager instance

    RC createFile    (const string &fileName);                            // Create a new file
    RC destroyFile   (const string &fileName);                            // Destroy a file
    RC openFile      (const string &fileName, FileHandle &fileHandle);    // Open a file
    RC closeFile     (FileHandle &fileHandle);                            // Close a file
    //RC forceFflush	 (FileHandle &fileHandle);
    static set<string> openedFileList;
   // static set<string> existedFileList;
protected:
    PagedFileManager();                                                   // Constructor
    ~PagedFileManager();                                                  // Destructor

private:
    static PagedFileManager *_pf_manager; // Only one in whole program
};



class FileHandle
{
public:
    // variables to keep the counter for each operation
	// Even file is closed, these 3 parameters below need to be persistent; where?
	// how large the file is: should be stored somewhere; where?
		// one way to do is: allocate the first page of the file as reserved space, to store the size of file or other information

    unsigned readPageCounter;
    unsigned writePageCounter;
    unsigned appendPageCounter;
    int fileState; // -1 deleted; 0 closed; 1 opened;

    FILE* fileHandlePf;
    string fileName;
    
    FileHandle();                                                         // Default constructor
    ~FileHandle();                                                        // Destructor

    RC readPage(PageNum pageNum, void *data);                             // Get a specific page //from pageNum to override data
    RC writePage(PageNum pageNum, const void *data);                      // Write a specific page
    RC appendPage(const void *data);                                      // Append a specific page  //The only way to grow files
    unsigned getNumberOfPages();                                          // Get the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);  // Put the current counter values into variables
    RC setPf(FILE* pt);

	RC initialCnt();
	RC forceFflush();


}; 

#endif
