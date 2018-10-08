#ifndef _ix_h_
#define _ix_h_

#include <iostream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <stdexcept>
#include <stdio.h>
#include <math.h>
#include <vector>
#include <algorithm>
#include <bitset>
#include <stack>
#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

typedef unsigned int pageNumber;
typedef unsigned int PageSize;

typedef struct
{
	int nextSilbingPageNum;
	int nodeType;
	int numOfKey;
	int freeSpace;
}nodeDescriptor;

const int NONLEAF=1;
const int LEAF=3;

class IX_ScanIterator;
class IXFileHandle;

struct treeNode{
	treeNode(int pgNum, int ofSt){
		pageNum = pgNum;
		offset = ofSt;
	}
	int pageNum;
	int offset;
};

class IndexManager {

    public:
        static IndexManager* instance();

        static int compareKey(const Attribute &attribute, const void *key, int ridPgNum, int ridSltNum,
        		const void* keyInPage, int keyInPage_pgNum, int keyInPage_sltNum);

        static RC readPageInfo(void* currentPage, int &nextLeafPageNum, int &pageType,
        		int &rcrdCount, int &freeSpace);

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);


        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
        void prtKey( void* varValue,const Attribute &attribute, void* &preKey,
        		void* currentKey, int offset, const RID &rid) const;

        RC updatePageInfo(void* currentPage, int nextLeafPageNum, int pageType, int newRcrdCount, int newFreeSpace);

        RC findLeafPos(IXFileHandle &ixfileHandle,  const Attribute &attribute,
        		const void *key, int ridPgNum, int ridSltNum, void* currentPage,
				int &targetPageNum, int &targetOffset, stack<treeNode> &parentStkList);

        RC insertIntoLeaf(IXFileHandle &ixfileHandle, const int targetPageNum, const int targetOffset,
        		const void* key, const RID &rid, const Attribute &attribute, int lenKey );

        RC splitAndInsertIntoLeaf(IXFileHandle &ixfileHandle, const int targetPageNum, const int targetOffset,
        		const void* key, const RID &rid, const Attribute &attribute,
				stack<treeNode> &parentStkList, int lenKey );

        RC insertIntoNode(IXFileHandle &ixfileHandle, const int targetPageNum, const int targetOffset,
        		const void* firstKeyWithRid, const int lenOfFirstKeyWithRid, const int splittedPageNum,
				const Attribute &attribute);

        RC splitAndInsertIntoNode(IXFileHandle &ixfileHandle, const int targetPageNum, const int targetOffset,
        		const void* firstKeyWithRid, const int lenOfFirstKeyWithRid, const int splittedPageNum,
				const Attribute &attribute, stack<treeNode> &parentStkList);

        RC printPageContent(int currentPageNum, IXFileHandle &ixfileHandle, const Attribute &attribute) const;

        RC findEquLeafPos(IXFileHandle &ixfileHandle,  const Attribute &attribute, const void *key,
        		const RID &rid, void* currentPage, int &targetPageNum, int &targetOffset,
				stack<treeNode> &parentStkList);

        RC findLeafPosExactEqu(IXFileHandle &ixfileHandle, const Attribute &attribute,
        		const void *key, const RID &rid, void* currentPage, int &targetPageNum,
        		int &targetOffset, stack<treeNode> &parentStkList);

        int getLenOfKey(const Attribute &attribute, const void *KeyPt); // for varchar return 4+realLen; for real and int return 4
    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        PagedFileManager* ix_pfm;

        void printBtreeNode(IXFileHandle &ixfileHandle, const Attribute &attribute,int layer,pageNumber nodeNumber) const;
        void prtKey( void* varValue,const Attribute &attribute) const;
};



class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

	FileHandle fileHandle;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
    unsigned int getRootPageNum();
    RC readPage(pageNumber pageNum, void* data);
    RC writePage(pageNumber pageNum, void* data);


    private:

};

class IX_ScanIterator {
    public:
		IXFileHandle* ixfileHandlePt;
		Attribute* attributePt;
		void* lowKey;
		void* highKey;
		void* lastKey;


		int lastPageNum;
		int lastSlotNum;
		int highKeyRidPgNum;
		int highKeyRidSltNum;
	    int lowKeyRidPgNum;
	    int lowKeyRidSltNum;
		bool lowKeyInclusive;
		bool highKeyInclusive;

		int currentPageNum;
		int currentOffset;

		unsigned lastWritePageCount;
		int lastLenWholeKey;

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();


};



#endif
