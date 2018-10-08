
#include "ix.h"
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
using namespace std;
IndexManager* IndexManager::_index_manager = 0;
//PagedFileManager* IndexManager::ix_pfm = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager){
        _index_manager = new IndexManager();
        _index_manager->ix_pfm = PagedFileManager::instance();
    }

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
	return(ix_pfm->createFile(fileName));
}

RC IndexManager::destroyFile(const string &fileName)
{
    return(ix_pfm->destroyFile(fileName));
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
	// how to determine whether the file is already opened?
	return(ix_pfm->openFile(fileName, ixfileHandle.fileHandle));
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return(ix_pfm->closeFile(ixfileHandle.fileHandle));
}

RC IndexManager::updatePageInfo(void* currentPage, int nextLeafPageNum, int pageType, int newRcrdCount, int newFreeSpace){
	// <0 means no need to change
	char* tmpPt;
	tmpPt = (char*)currentPage + PAGE_SIZE - 4*4;
	memcpy(tmpPt, &nextLeafPageNum, 4);
	tmpPt += 4;
	memcpy(tmpPt, &pageType, 4);
	tmpPt += 4;
	memcpy(tmpPt, &newRcrdCount, 4);
	tmpPt += 4;
	memcpy(tmpPt, &newFreeSpace, 4);
	return 0;
}

// for VarChr, keyInPage key are both whole key (for varchar include 4Byte length)
// return compareKye(key1, key2):
// -1: key1<key2
// 0: key1=key2
// 1: key1>key2
// 3: key1 is NULL
// 4: key2 is NULL
int IndexManager::compareKey(const Attribute &attribute, const void *key, int ridPgNum, int ridSltNum,
		const void* keyInPage, int keyInPage_pgNum, int keyInPage_sltNum){
	bool flagEqu = false;
	bool flagSml = false;
	bool flagBig = false;
	int compKey = INT_MAX;

	if(key==NULL) return 3;
	if(keyInPage==NULL) return 4;

	if(attribute.type==TypeVarChar){
		char* insertKey = (char*)(malloc(PAGE_SIZE));
		memset(insertKey, 0, PAGE_SIZE);
		int insertKeyLen = 0;
		memcpy(&insertKeyLen, key, 4);
		memcpy(insertKey, (char*)key+4, insertKeyLen);
		string insertKeyStr = insertKey;

		char* realKeyInPage = (char*)(malloc(PAGE_SIZE));
		memset(realKeyInPage, 0, PAGE_SIZE);
		int realKeyInPageLen = 0;
		memcpy(&realKeyInPageLen, keyInPage, 4);
		memcpy(realKeyInPage, (char*)keyInPage+4, realKeyInPageLen);
		string keyInPageStr = realKeyInPage;

		flagEqu = (insertKeyStr==keyInPageStr);
		flagSml = (insertKeyStr<keyInPageStr);
		//flagBig = (insertKeyStr>keyInPageStr);

		free(insertKey);
		free(realKeyInPage);
	}
	else if(attribute.type==TypeInt){
		int insertKeyInt = 0;
		memcpy(&insertKeyInt, key, 4);
		int keyInPageInt = 0;
		memcpy(&keyInPageInt, keyInPage, 4);
		flagEqu = (insertKeyInt==keyInPageInt);
		flagSml = (insertKeyInt<keyInPageInt);
		//flagBig = (insertKeyInt>keyInPageInt);
	}
	else if(attribute.type==TypeReal){
		float insertKeyFlt = 0;
		memcpy(&insertKeyFlt, key, 4);
		float keyInPageFlt = 0;
		memcpy(&keyInPageFlt, keyInPage, 4);
		flagEqu = (insertKeyFlt==keyInPageFlt);
		flagSml = (insertKeyFlt<keyInPageFlt);
		//flagBig = (insertKeyFlt>keyInPageFlt);
	}

	if(flagEqu == true) {
		// If key equ, then determine by rid
		if(ridPgNum==keyInPage_pgNum){
			if (ridSltNum==keyInPage_sltNum) compKey = 0;
			else{
				if(ridSltNum<keyInPage_sltNum) compKey = -1;
				else compKey = 1;
			}
		}
		else{
			if (ridPgNum<keyInPage_pgNum) compKey = -1;
			else compKey = 1;
		}
	}
	else{
		if(flagSml == true)	compKey = -1;
		else compKey = 1;
	}


	return compKey;
}

// return the start position (pageNum, offset) of the first (keyInPage ridInPage) which is bigger than (key rid)
RC IndexManager::findLeafPos(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *key, int ridPgNum, int ridSltNum, void* currentPage, int &targetPageNum,
		int &targetOffset, stack<treeNode> &parentStkList){
	int currentPageNum = targetPageNum;
	char* slotStartPt = (char*)currentPage + PAGE_SIZE - 4*3;
	int pageType = 0;
	memcpy(&pageType, slotStartPt, 4);
	int rcrdCountN = 0;
	memcpy(&rcrdCountN, slotStartPt+4, 4);
	int freeSpaceF = 0;
	memcpy(&freeSpaceF, slotStartPt+8, 4);
	char* tmpPt = (char*)currentPage;
	char* keyInPage = (char*)(malloc(PAGE_SIZE));
	memset(keyInPage, 0, PAGE_SIZE);
	int tmpKeyLen = 0;
	int rcrdLen = 0;
	int rcrdCnt = 1;
	int keyInPage_pgNum = 0;
	int keyInPage_sltNum = 0;
	// if this is leafNode
	if(pageType==3){
		targetOffset = 0;
		if(rcrdCountN==0){
			free(keyInPage);
			return 0;
		}
		else{
			while(rcrdCnt<=rcrdCountN){
				if(attribute.type==TypeVarChar){
					memcpy(&tmpKeyLen, tmpPt, 4);
					tmpPt += 4;
					memcpy(keyInPage, tmpPt - 4, tmpKeyLen + 4);
					tmpPt += tmpKeyLen;
					rcrdLen = 4+tmpKeyLen;

				}
				else{
					tmpKeyLen = 4;
					memcpy(keyInPage, tmpPt, tmpKeyLen);
					tmpPt += tmpKeyLen;
					rcrdLen = 4;
				}

				// read rid of keyInPage
				memcpy(&keyInPage_pgNum, tmpPt, 4);
				tmpPt += 4;
				memcpy(&keyInPage_sltNum, tmpPt, 4);
				tmpPt += 4;

				int tmpFlag = compareKey(attribute, key, ridPgNum, ridSltNum, keyInPage, keyInPage_pgNum, keyInPage_sltNum);
				// if key<keyInPage || key isNull
				if(tmpFlag==-1 || tmpFlag==3){
					free(keyInPage);
					return 0;
				}
				else{
					targetOffset += rcrdLen + 4 + 4;
					rcrdCnt++;
				}
			}
			free(keyInPage);
			return 0;
		}
	}
	// if this isnot leafNode
	else{
		memcpy(&targetPageNum, currentPage, 4);
		tmpPt += 4;
		targetOffset = 4;

		void* nextPage = malloc(PAGE_SIZE);
		memset(nextPage, 0, PAGE_SIZE);

		while(rcrdCnt<=rcrdCountN){
			if(attribute.type==TypeVarChar){
				memcpy(&tmpKeyLen, tmpPt, 4);
				tmpPt += 4;
				memcpy(keyInPage, tmpPt - 4, tmpKeyLen + 4);
				tmpPt += tmpKeyLen;
				rcrdLen = 4+tmpKeyLen;
			}
			else{
				tmpKeyLen = 4;
				memcpy(keyInPage, tmpPt, tmpKeyLen);
				tmpPt += tmpKeyLen;
				rcrdLen = 4;
			}

			// read rid of keyInPage
			memcpy(&keyInPage_pgNum, tmpPt, 4);
			tmpPt += 4;
			memcpy(&keyInPage_sltNum, tmpPt, 4);
			tmpPt += 4;

			int tmpFlag = compareKey(attribute, key, ridPgNum, ridSltNum, keyInPage, keyInPage_pgNum, keyInPage_sltNum);
			if(tmpFlag==-1 || tmpFlag==3){
				parentStkList.push(treeNode(currentPageNum, targetOffset));
				memset(nextPage, 0, PAGE_SIZE);
				ixfileHandle.fileHandle.readPage(targetPageNum, nextPage);

				int tmpRes = findLeafPos(ixfileHandle, attribute, key, ridPgNum, ridSltNum, nextPage,
						targetPageNum, targetOffset, parentStkList);

				free(keyInPage);
				free(nextPage);
				return tmpRes;
			}
			else{
				targetOffset += rcrdLen + 4 + 4 + 4; // move to "before next rcrd"
				memcpy(&targetPageNum, tmpPt, 4); // update targetPageNum
				tmpPt += 4;
				rcrdCnt++;
			}
		}
		parentStkList.push(treeNode(currentPageNum, targetOffset));
		memset(nextPage, 0, PAGE_SIZE);
		ixfileHandle.fileHandle.readPage(targetPageNum, nextPage);
		int tmpRes = findLeafPos(ixfileHandle, attribute, key, ridPgNum, ridSltNum, nextPage,
				targetPageNum, targetOffset, parentStkList);
		free(keyInPage);
		free(nextPage);
		return tmpRes;
	}

	return 0;
}



RC IndexManager::findLeafPosExactEqu(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *key, const RID &rid, void* currentPage, int &targetPageNum,
		int &targetOffset, stack<treeNode> &parentStkList){
	int currentPageNum = targetPageNum;
	char* slotStartPt = (char*)currentPage + PAGE_SIZE - 4*3;
	int pageType = 0;
	memcpy(&pageType, slotStartPt, 4);
	int rcrdCountN = 0;
	memcpy(&rcrdCountN, slotStartPt+4, 4);
	int freeSpaceF = 0;
	memcpy(&freeSpaceF, slotStartPt+8, 4);
	char* tmpPt = (char*)currentPage;
	char* keyInPage = (char*)(malloc(PAGE_SIZE));
	memset(keyInPage, 0, PAGE_SIZE);
	int tmpKeyLen = 0;
	int rcrdLen = 0;
	int rcrdCnt = 1;
	int keyInPage_pgNum = 0;
	int keyInPage_sltNum = 0;
	// if this is leafNode
	if(pageType==3){
		targetOffset = 0;
		if(rcrdCountN==0){
			free(keyInPage);
			return -1;
		}
		else{
			while(rcrdCnt<=rcrdCountN){
				if(attribute.type==TypeVarChar){
					memcpy(&tmpKeyLen, tmpPt, 4);
					tmpPt += 4;
					memcpy(keyInPage, tmpPt - 4, tmpKeyLen + 4);
					tmpPt += tmpKeyLen;
					rcrdLen = 4+tmpKeyLen;

				}
				else{
					tmpKeyLen = 4;
					memcpy(keyInPage, tmpPt, tmpKeyLen);
					tmpPt += tmpKeyLen;
					rcrdLen = 4;
				}

				// read rid of keyInPage
				memcpy(&keyInPage_pgNum, tmpPt, 4);
				tmpPt += 4;
				memcpy(&keyInPage_sltNum, tmpPt, 4);
				tmpPt += 4;

				int tmpFlag = compareKey(attribute, key, rid.pageNum, rid.slotNum, keyInPage, keyInPage_pgNum, keyInPage_sltNum);

				if(tmpFlag==0){
					free(keyInPage);
					return 0;
				}
				else{
					if(tmpFlag<0) return -1;
					else{
						targetOffset += rcrdLen + 4 + 4;
						rcrdCnt++;
					}
				}
			}
			free(keyInPage);
			return -1;
		}
	}
	// if this isnot leafNode
	else{
		memcpy(&targetPageNum, currentPage, 4);
		tmpPt += 4;
		targetOffset = 4;

		void* nextPage = malloc(PAGE_SIZE);
		memset(nextPage, 0, PAGE_SIZE);

		while(rcrdCnt<=rcrdCountN){
			if(attribute.type==TypeVarChar){
				memcpy(&tmpKeyLen, tmpPt, 4);
				tmpPt += 4;
				memcpy(keyInPage, tmpPt - 4, tmpKeyLen + 4);
				tmpPt += tmpKeyLen;
				rcrdLen = 4+tmpKeyLen;
			}
			else{
				tmpKeyLen = 4;
				memcpy(keyInPage, tmpPt, tmpKeyLen);
				tmpPt += tmpKeyLen;
				rcrdLen = 4;
			}

			// read rid of keyInPage
			memcpy(&keyInPage_pgNum, tmpPt, 4);
			tmpPt += 4;
			memcpy(&keyInPage_sltNum, tmpPt, 4);
			tmpPt += 4;

			int tmpFlag = compareKey(attribute, key, rid.pageNum, rid.slotNum, keyInPage, keyInPage_pgNum, keyInPage_sltNum);
			if(tmpFlag==-1 || tmpFlag==3){
				parentStkList.push(treeNode(currentPageNum, targetOffset));
				memset(nextPage, 0, PAGE_SIZE);
				ixfileHandle.fileHandle.readPage(targetPageNum, nextPage);
				int tmpRes = findLeafPosExactEqu(ixfileHandle, attribute, key, rid, nextPage, targetPageNum, targetOffset, parentStkList);
				free(nextPage);
				free(keyInPage);
				return tmpRes;
			}
			else{
				targetOffset += rcrdLen + 4 + 4 + 4; // move to "before next rcrd"
				memcpy(&targetPageNum, tmpPt, 4); // update targetPageNum
				tmpPt += 4;
				rcrdCnt++;
			}
		}
		parentStkList.push(treeNode(currentPageNum, targetOffset));
		memset(nextPage, 0, PAGE_SIZE);
		ixfileHandle.fileHandle.readPage(targetPageNum, nextPage);
		int tmpRes = findLeafPosExactEqu(ixfileHandle, attribute, key, rid, nextPage, targetPageNum, targetOffset, parentStkList);
		free(nextPage);
		free(keyInPage);
		return tmpRes;
	}

	return -1;
}

RC IndexManager::findEquLeafPos(IXFileHandle &ixfileHandle,  const Attribute &attribute, const void *key,
		const RID &rid, void* currentPage, int &targetPageNum, int &targetOffset, stack<treeNode> &parentStkList){

	int tmpFlag = findLeafPosExactEqu(ixfileHandle, attribute, key, rid, currentPage, targetPageNum,
			targetOffset, parentStkList);
	return tmpFlag;

}

RC IndexManager::readPageInfo(void* currentPage, int &nextLeafPageNum, int &pageType, int &rcrdCount, int &freeSpace){
	char* tmpPt;
	tmpPt = (char*)currentPage + PAGE_SIZE - 4*4;
	memcpy(&nextLeafPageNum, tmpPt, 4);
	tmpPt += 4;
	memcpy(&pageType, tmpPt, 4);
	tmpPt += 4;
	memcpy(&rcrdCount, tmpPt, 4);
	tmpPt += 4;
	memcpy(&freeSpace, tmpPt, 4);

	if(pageType!=3) nextLeafPageNum = -1;
	return 0;
}
RC IndexManager::insertIntoLeaf(IXFileHandle &ixfileHandle, const int targetPageNum, const int targetOffset,
		const void* key, const RID &rid, const Attribute &attribute, int lenKey ){
	void* currentPage = malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(targetPageNum, currentPage);
	// 4*2 means the rid.pageNum and rid.slotNum stored after key
	int lenOfMovSeq = PAGE_SIZE - targetOffset - 4*4 - lenKey  - 4*2;
	int lenSift = lenKey + 4*2;
	char* movPt = (char*)currentPage + targetOffset;
	char* desPt = movPt + lenSift;
	memmove(desPt, movPt, lenOfMovSeq);

	char* tmpPt = movPt;
	memcpy(tmpPt, key, lenKey);
	tmpPt += lenKey;
	memcpy(tmpPt, &(rid.pageNum), 4);
	tmpPt += 4;
	memcpy(tmpPt, &(rid.slotNum), 4);

	int nextLeafPageNum = -1;
	int pageType = -1;
	int rcrdCount = -1;
	int freeSpace = -1;
	readPageInfo(currentPage, nextLeafPageNum, pageType, rcrdCount, freeSpace);
	updatePageInfo(currentPage, nextLeafPageNum, pageType, rcrdCount+1, freeSpace-lenSift);
	ixfileHandle.fileHandle.writePage(targetPageNum, currentPage);
	free(currentPage);

	return 0;
}

RC IndexManager::insertIntoNode(IXFileHandle &ixfileHandle, const int targetPageNum, const int targetOffset,
		const void* firstKeyWithRid, const int lenOfFirstKeyWithRid, const int splittedPageNum,
		const Attribute &attribute){
	void* targetPage = malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(targetPageNum, targetPage);
	int lenOfMoveSeq = PAGE_SIZE - targetOffset - 4*4 - lenOfFirstKeyWithRid - 4; // 4 pgNum
	int lenSift = lenOfFirstKeyWithRid + 4;
	char* movPt = (char*)targetPage + targetOffset;
	char* targetPt = movPt + lenSift;
	memmove(targetPt, movPt, lenOfMoveSeq);

	char* tmpPt = movPt;
	memcpy(tmpPt, firstKeyWithRid, lenOfFirstKeyWithRid);
	tmpPt += lenOfFirstKeyWithRid;
	memcpy(tmpPt, &splittedPageNum, 4);

	int nextLeafPageNum = -1;
	int pageType = -1;
	int rcrdCount = -1;
	int freeSpace = -1;
	readPageInfo(targetPage, nextLeafPageNum, pageType, rcrdCount, freeSpace);
	updatePageInfo(targetPage, nextLeafPageNum, pageType, rcrdCount+1, freeSpace-lenSift);
	ixfileHandle.fileHandle.writePage(targetPageNum, targetPage);

	free(targetPage);
	return 0;
}

RC IndexManager::splitAndInsertIntoNode(IXFileHandle &ixfileHandle, const int targetPageNum, const int targetOffset,
		const void* firstKeyWithRid, const int lenOfFirstKeyWithRid, const int splittedPageNum,
		const Attribute &attribute, stack<treeNode> &parentStkList){
// splittedPageNum should follow firstKeyWithRid to be inserted into (targetPageNum, targetOffset)
	//cout<<"splitAndInsertIntoNode() called! @Page "<<targetPageNum<<endl;
	void* currentPage = malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(targetPageNum, currentPage);
	int nextLeafPageNum = -1;
	int pageType = -1;
	int rcrdCount = -1;
	int freeSpace = -1;

	int rcrdCntPart1 = 0;
	int rcrdCntPart2 = 0;

	readPageInfo(currentPage, nextLeafPageNum, pageType, rcrdCount, freeSpace);

	int splitSizeStd = (PAGE_SIZE - freeSpace + lenOfFirstKeyWithRid + 4 + 4*4) / 2;
	int splitOffset = 4;
	char* tmpPt = (char*)currentPage; // pgLeft, jump over
	if(attribute.type == TypeVarChar){
		int* tmpLenKey = new int(0);
		while(splitOffset < splitSizeStd){
			memcpy(tmpLenKey, (char*)currentPage + splitOffset, 4);
			splitOffset += 4 + *tmpLenKey + 4*3;
			rcrdCntPart1 ++;
		}

	}
	else{
		splitOffset = ((splitSizeStd-4) / (4*4)) * (4*4) + 4;
		rcrdCntPart1 = (splitOffset-4) / (4*4);
	}
	rcrdCntPart2 = rcrdCount - rcrdCntPart1;

	int sizePart1 = splitOffset;
	int sizePart2 = PAGE_SIZE - freeSpace - 4*4 - splitOffset;


	// get the first [pageNum, key] at the beginning of part 2, which should sift up
	void* currentFirstKeyUp = malloc(PAGE_SIZE);  // include key, rid.pgNum, rid.sltNum
	int lenCurrentFirstKeyUp = 0;  // include key, rid.pgNum, rid.sltNum
	if(attribute.type == TypeVarChar){
		int* tmpLenKey = new int(0);
		memcpy(tmpLenKey, (char*)currentPage + splitOffset, 4);
		memcpy(currentFirstKeyUp, (char*)currentPage + splitOffset, 4 + *tmpLenKey + 4 + 4);
		lenCurrentFirstKeyUp = 4 + *tmpLenKey + 4 + 4;
	}
	else{
		memcpy(currentFirstKeyUp, (char*)currentPage + splitOffset, 4);
		lenCurrentFirstKeyUp = 12;
	}
	sizePart2 -= lenCurrentFirstKeyUp;
	rcrdCntPart2 -= 1;


	// write the split new page (part2) to file (except startPageNum, [ firstKey, firstKeyRidpgNum, firstKeyRidsltNum ] )
	void* splittedPage = malloc(PAGE_SIZE);
	memcpy(splittedPage, (char*)currentPage + splitOffset + lenCurrentFirstKeyUp, sizePart2);
	updatePageInfo(splittedPage, nextLeafPageNum, 2, rcrdCntPart2, PAGE_SIZE - sizePart2 - 4*4);
	ixfileHandle.fileHandle.appendPage(splittedPage);
	int currentSplittedPageNum = ixfileHandle.fileHandle.getNumberOfPages() - 1;

	// write part 1 to file
	memset((void*)((char*)currentPage + splitOffset), 0, PAGE_SIZE - sizePart1 - 4*4);
	updatePageInfo(currentPage, currentSplittedPageNum, 2, rcrdCntPart1, PAGE_SIZE - sizePart1 - 4*4);
	ixfileHandle.fileHandle.writePage(targetPageNum, currentPage);

	// insert the requested key-pageNum to part1 or part2 and write to file
	int newTargetOffset = 0;
	if(targetOffset <= splitOffset){
		newTargetOffset = targetOffset;
		insertIntoNode(ixfileHandle, targetPageNum, newTargetOffset, firstKeyWithRid, lenOfFirstKeyWithRid, splittedPageNum, attribute);
	}
	else{
		newTargetOffset = targetOffset - splitOffset - lenCurrentFirstKeyUp;
		insertIntoNode(ixfileHandle, currentSplittedPageNum, newTargetOffset, firstKeyWithRid, lenOfFirstKeyWithRid, splittedPageNum, attribute);
	}


	// send the currentFirstKeyUp up sift to parent node
	if(pageType!=1){
		treeNode parentNode = parentStkList.top();
		parentStkList.pop();
		void* parentPage = malloc(PAGE_SIZE);
		ixfileHandle.fileHandle.readPage(parentNode.pageNum, parentPage);
		int prNextLeafPageNum = -1;
		int prPageType = -1;
		int prRcrdCount = -1;
		int prFreeSpace = -1;
		readPageInfo(parentPage, prNextLeafPageNum, prPageType, prRcrdCount, prFreeSpace);
		if(prFreeSpace - (lenCurrentFirstKeyUp + 4) >= 4){
			insertIntoNode(ixfileHandle, parentNode.pageNum, parentNode.offset, currentFirstKeyUp, lenCurrentFirstKeyUp, currentSplittedPageNum, attribute);
		}
		else{
			splitAndInsertIntoNode(ixfileHandle, parentNode.pageNum, parentNode.offset, currentFirstKeyUp, lenCurrentFirstKeyUp, currentSplittedPageNum, attribute, parentStkList);
		}
		free(parentPage);
	}
	// if pageType==1 (or parentStk should be empty), it's root now, create a new root to store currentFirstKeyUp while keep the root pageNum = 0
	else{
		ixfileHandle.fileHandle.appendPage(currentPage);
		int tmpNewLeftsonPgNum = ixfileHandle.fileHandle.getNumberOfPages()-1;
		void* tmpNewRootPage = malloc(PAGE_SIZE);
		memcpy(tmpNewRootPage, &tmpNewLeftsonPgNum, 4);
		memcpy((char*)tmpNewRootPage + 4, currentFirstKeyUp, lenCurrentFirstKeyUp);
		memcpy((char*)tmpNewRootPage + 4 + lenCurrentFirstKeyUp, &currentSplittedPageNum, 4);
		updatePageInfo(tmpNewRootPage, -1, 1, 1, PAGE_SIZE - 4 - lenCurrentFirstKeyUp - 4);
		ixfileHandle.fileHandle.writePage(0, tmpNewRootPage);

		free(tmpNewRootPage);
	}


	free(splittedPage);
	free(currentFirstKeyUp);
	free(currentPage);

	return 0;
}


RC IndexManager::splitAndInsertIntoLeaf(IXFileHandle &ixfileHandle, const int targetPageNum, const int targetOffset, const void* key,
		const RID &rid, const Attribute &attribute, stack<treeNode> &parentStkList, int lenKey ){

	//cout<<"splitAndInsertIntoLeaf() called! @Page "<<targetPageNum<<endl;
	void* currentPage = malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(targetPageNum, currentPage);
	int splitOffset = 0;
	int nextLeafPageNum = -1;
	int pageType = -1;
	int rcrdCount = -1;
	int freeSpace = -1;

	int rcrdCntPart1 = 0;
	int rcrdCntPart2 = 0;

	readPageInfo(currentPage, nextLeafPageNum, pageType, rcrdCount, freeSpace);

	int splitSizeStd = PAGE_SIZE / 2;
	char* tmpPt = (char*)currentPage;
	if(attribute.type == TypeVarChar){
		int* tmpLenKey = new int(0);
		while(splitOffset < splitSizeStd){
			memcpy(tmpLenKey, (char*)currentPage + splitOffset, 4);
			splitOffset += 4 + *tmpLenKey + 4*2;
			rcrdCntPart1 ++;
		}

	}
	else{
		splitOffset = (splitSizeStd / (4*3)) * (4*3);
		rcrdCntPart1 = splitOffset / (4*3);
	}

	rcrdCntPart2 = rcrdCount - rcrdCntPart1;

	int sizePart1 = splitOffset;
	int sizePart2 = PAGE_SIZE - freeSpace - 4*4 - splitOffset;


	// write the split new page (part2) to file
	void* splittedPage = malloc(PAGE_SIZE);
	memcpy(splittedPage, (char*)currentPage + splitOffset, sizePart2);
	updatePageInfo(splittedPage, nextLeafPageNum, 3, rcrdCntPart2, PAGE_SIZE - sizePart2 - 4*4);
	ixfileHandle.fileHandle.appendPage(splittedPage);
	int splittedPageNum = ixfileHandle.fileHandle.getNumberOfPages() - 1;


	//write the old page (part1) to file
	memset((void*)((char*)currentPage + splitOffset), 0, PAGE_SIZE - sizePart1 - 4*4);
	updatePageInfo(currentPage, splittedPageNum, 3, rcrdCntPart1, PAGE_SIZE - sizePart1 - 4*4);
	ixfileHandle.fileHandle.writePage(targetPageNum, currentPage);

	//insert the key
	int newTargetOffset = 0;
	if(targetOffset < splitOffset){
		newTargetOffset = targetOffset;
		insertIntoLeaf(ixfileHandle, targetPageNum, newTargetOffset, key, rid, attribute, lenKey );
	}
	else{
		newTargetOffset = targetOffset - splitOffset;
		insertIntoLeaf(ixfileHandle, splittedPageNum, newTargetOffset, key, rid, attribute, lenKey );
	}

	// get the first key in splitted page (and read to insert it into parentNode)
	tmpPt = (char*)splittedPage;
	void* firstKeyWithRid = (malloc(PAGE_SIZE));
	int lenOfFirstKeyWithRid = 0;
	if(attribute.type == TypeVarChar){
		int* tmpLenKey = new int(0);
		memcpy(tmpLenKey, tmpPt, 4);
		lenOfFirstKeyWithRid = 4 + *tmpLenKey + 4*2;
	}
	else{
		lenOfFirstKeyWithRid = 4 + 4*2;
	}
	memcpy(firstKeyWithRid, splittedPage, lenOfFirstKeyWithRid);

	// get the parentPage and pageInfo, insert the left-first key in right-splitted page up to parentNode.
	if(parentStkList.empty()==false){
		treeNode parentNode = parentStkList.top();
		parentStkList.pop();
		void* parentPage = malloc(PAGE_SIZE);
		ixfileHandle.fileHandle.readPage(parentNode.pageNum, parentPage);
		int prNextLeafPageNum = -1;
		int prPageType = -1;
		int prRcrdCount = -1;
		int prFreeSpace = -1;
		readPageInfo(parentPage, prNextLeafPageNum, prPageType, prRcrdCount, prFreeSpace);
		if(prFreeSpace - (lenOfFirstKeyWithRid + 4) >= 4){
			insertIntoNode(ixfileHandle, parentNode.pageNum, parentNode.offset, firstKeyWithRid, lenOfFirstKeyWithRid, splittedPageNum, attribute);
		}
		else{
			splitAndInsertIntoNode(ixfileHandle, parentNode.pageNum, parentNode.offset, firstKeyWithRid, lenOfFirstKeyWithRid, splittedPageNum, attribute, parentStkList);
		}
	}
	// If no parent, then it's the first page splitting, should conduct a new root node (@pgNum 0 )
	else{
		ixfileHandle.fileHandle.readPage(0, currentPage); // I missed this line and spent tens of hours to realize... fxk!
		ixfileHandle.fileHandle.appendPage(currentPage);
		int tmpNewLeftsonPgNum = ixfileHandle.fileHandle.getNumberOfPages()-1;
		void* tmpNewRootPage = malloc(PAGE_SIZE);
		memcpy(tmpNewRootPage, &tmpNewLeftsonPgNum, 4);
		memcpy((char*)tmpNewRootPage + 4, firstKeyWithRid, lenOfFirstKeyWithRid);
		memcpy((char*)tmpNewRootPage + 4 + lenOfFirstKeyWithRid, &splittedPageNum, 4);
		updatePageInfo(tmpNewRootPage, -1, 1, 1, PAGE_SIZE - 4 - lenOfFirstKeyWithRid - 4 - 16);
		ixfileHandle.fileHandle.writePage(0, tmpNewRootPage);

		free(tmpNewRootPage);

	}

	free(firstKeyWithRid);
	free(splittedPage);
	free(currentPage);
	return 0;
}
RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
////////////////
/*	if(attribute.type == TypeInt)	cout<<"insert "<<ixfileHandle.fileHandle.fileName<<" "<<*(int*)key<<" rid: "<<rid.pageNum<<" "<<rid.slotNum<<endl;
	else if(attribute.type == TypeReal)	cout<<"insert "<<ixfileHandle.fileHandle.fileName<<" "<<*(float*)key<<" rid: "<<rid.pageNum<<" "<<rid.slotNum<<endl;
	else cout<<"insert "<<ixfileHandle.fileHandle.fileName<<" "<<*((char*)key + 4)<<" rid: "<<rid.pageNum<<" "<<rid.slotNum<<endl;
*/
	////////////////
	if (ixfileHandle.fileHandle.getNumberOfPages() == 0){
		void* firstPage = malloc(PAGE_SIZE);
		memset(firstPage, 0, PAGE_SIZE);
		ixfileHandle.fileHandle.appendPage(firstPage);
		free(firstPage);
	}
	void* rootPage = malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(0, rootPage);
	int* rcrdCount = (int*)((char*)rootPage+PAGE_SIZE-8);
	int* freeSpace = (int*)((char*)rootPage+PAGE_SIZE-4);
	int lenKey; // byte of total key ( for varchar, include 4byte length)
	if(attribute.type==TypeVarChar){
		int tmpLenChar = 0;
		memcpy(&tmpLenChar, key, 4);
		lenKey = tmpLenChar+4;
	}
	else{
		lenKey = 4;
	}

	// new Tree
	if(*rcrdCount == 0){
		char* tmpPt = (char*)rootPage;

		// @rootPage

		// leftChildPgNum (4Byte)
		int pg0 = 1;
		memcpy(tmpPt, &pg0, 4);
		tmpPt += 4;

		//void* rightChildOfRoot = malloc(PAGE_SIZE);
		memset(rootPage, 0, PAGE_SIZE);
		tmpPt = (char*)rootPage;
		memcpy(tmpPt, key, lenKey);
		tmpPt += lenKey;
		memcpy(tmpPt, &(rid.pageNum), 4);
		tmpPt += 4;
		memcpy(tmpPt, &(rid.slotNum), 4);
		tmpPt += 4;
		updatePageInfo(rootPage, -1, 3, 1, PAGE_SIZE - lenKey - 4 - 4 - 4*4);
		ixfileHandle.writePage(0, rootPage);

		//free(leftChildOfRoot);
		//free(rightChildOfRoot);
		free(rootPage);
		return 0;
	}
	// not new Tree
	else{
		int targetPageNum = 0;
		int targetOffset = 0;
		stack<treeNode> parentStkList;
		findLeafPos(ixfileHandle, attribute, key, rid.pageNum, rid.slotNum, rootPage, targetPageNum, targetOffset, parentStkList);

		//if(freeSpace+lenKey<PAGE_SIZE) { insertIntoLeaf(); }
		//else { splitAndInsert(); // recursive and maintain parent (pageNum, offset) stack }
		void* targetPage = malloc(PAGE_SIZE);
		ixfileHandle.fileHandle.readPage(targetPageNum, targetPage);
		char* tmpPt = (char*)targetPage;
		int* nextLeafPageNumPt = (int*)((char*)targetPage + PAGE_SIZE - 4*4);
		int* paegTypePt = (int*)((char*)targetPage + PAGE_SIZE - 4*3);
		int* rcrdCountPt = (int*)((char*)targetPage + PAGE_SIZE - 4*2);
		int* freeSpacePt = (int*)((char*)targetPage + PAGE_SIZE - 4);

		if(*freeSpacePt - lenKey - 4*2 > 4) insertIntoLeaf(ixfileHandle, targetPageNum, targetOffset, key, rid, attribute, lenKey );
		else splitAndInsertIntoLeaf(ixfileHandle, targetPageNum, targetOffset, key, rid, attribute, parentStkList, lenKey );


		free(targetPage);
		free(rootPage);
		return 0;
	}
    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	int deleteKeyOffset=0;
	RC findToDeleteKey=-1;
	stack<treeNode> parentStkList;
	pageNumber rootPage=0;
	void* currentPage=malloc(PAGE_SIZE);
	memset(currentPage,0,PAGE_SIZE);
	int targetPageNum=0;
	void* deletePage=malloc(PAGE_SIZE);
	memset(deletePage,0,PAGE_SIZE);
	int moveByteNum=0;
	char* moveToPos=new char;
	char* moveFromPos=new char;
	char* cleanBytePos=new char;
	nodeDescriptor nodeDesc;
	int newKeyNum=0;
	int newFreeSpace=0;
	int varTotLength=0;

	ixfileHandle.readPage(rootPage,currentPage);
	findToDeleteKey=findEquLeafPos(ixfileHandle, attribute,key, rid, currentPage, targetPageNum, deleteKeyOffset, parentStkList);
	if(findToDeleteKey==0)
	{
		if(attribute.type==TypeVarChar)
		{
			ixfileHandle.readPage(targetPageNum,deletePage);
			memcpy(&nodeDesc,(char*)deletePage + PAGE_SIZE - 4*4, sizeof(nodeDescriptor));
			moveToPos=(char*)deletePage+deleteKeyOffset;
			varTotLength=4 + *(int*)moveToPos;

			moveByteNum=PAGE_SIZE - nodeDesc.freeSpace - 4*4 - deleteKeyOffset - varTotLength;
			moveFromPos=(char*)deletePage + deleteKeyOffset + varTotLength;
			memmove(moveToPos,moveFromPos,moveByteNum);
			cleanBytePos=(char*)deletePage + PAGE_SIZE - 4*4 -nodeDesc.freeSpace - varTotLength;
			memset(cleanBytePos,0,varTotLength);
			newKeyNum=nodeDesc.numOfKey - 1;
			newFreeSpace=nodeDesc.freeSpace + varTotLength;
			memcpy((char*)deletePage + PAGE_SIZE - 2*4,&newKeyNum,sizeof(int));
			memcpy((char*)deletePage + PAGE_SIZE - 4,&newFreeSpace,sizeof(int));
			ixfileHandle.writePage(targetPageNum,deletePage);
		}
		else if (attribute.type==TypeInt)
		{
			ixfileHandle.readPage(targetPageNum,deletePage);
			memcpy(&nodeDesc,(char*)deletePage + PAGE_SIZE - 4*4, sizeof(nodeDescriptor));
			moveToPos=(char*)deletePage+deleteKeyOffset;
			moveByteNum= 12*nodeDesc.numOfKey - deleteKeyOffset - 12;
			moveFromPos=(char*)deletePage+deleteKeyOffset+12;
			memmove(moveToPos,moveFromPos,moveByteNum);
			cleanBytePos=(char*)deletePage + 12*nodeDesc.numOfKey - 12;
			memset(cleanBytePos,0,12);
			newKeyNum=nodeDesc.numOfKey - 1;
			newFreeSpace=nodeDesc.freeSpace + 12;
			memcpy((char*)deletePage + PAGE_SIZE - 2*4,&newKeyNum,sizeof(int));
			memcpy((char*)deletePage + PAGE_SIZE - 4,&newFreeSpace,sizeof(int));
			ixfileHandle.writePage(targetPageNum,deletePage);
		}
		else if(attribute.type==TypeReal)
		{
			ixfileHandle.readPage(targetPageNum,deletePage);
			memcpy(&nodeDesc,(char*)deletePage + PAGE_SIZE - 4*4, sizeof(nodeDescriptor));
			moveToPos=(char*)deletePage+deleteKeyOffset;
			moveByteNum= 12*nodeDesc.numOfKey - deleteKeyOffset - 12;
			moveFromPos=(char*)deletePage+deleteKeyOffset+12;
			memmove(moveToPos,moveFromPos,moveByteNum);
			cleanBytePos=(char*)deletePage + 12*nodeDesc.numOfKey - 12;
			memset(cleanBytePos,0,12);
			newKeyNum=nodeDesc.numOfKey - 1;
			newFreeSpace=nodeDesc.freeSpace + 12;
			memcpy((char*)deletePage + PAGE_SIZE - 2*4,&newKeyNum,sizeof(int));
			memcpy((char*)deletePage + PAGE_SIZE - 4,&newFreeSpace,sizeof(int));
			ixfileHandle.writePage(targetPageNum,deletePage);

		}
		//ixfileHandle.fileHandle.forceFflush();
		free(deletePage);
		free(currentPage);
		return 0;

	}
	else
	{
		//cout<<"Can not find the wanted key!"<<endl;
		free(deletePage);
		free(currentPage);
		return -1;
	}

    return -1;
}




RC IndexManager::printPageContent(int currentPageNum, IXFileHandle &ixfileHandle,
		const Attribute &attribute) const{
	void* currentPage = malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(currentPageNum, currentPage);
	char* tmpPt = (char*)currentPage;

	int NextLeafPageNum = -1;
	int PageType = -1;
	int RcrdCount = -1;
	int FreeSpace = -1;
	int tmpPgNum = 0;
	void* tmpKey = malloc(PAGE_SIZE);
	memset(tmpKey, 0, PAGE_SIZE);

	readPageInfo(currentPage, NextLeafPageNum, PageType, RcrdCount, FreeSpace);
	cout<<"pageNum: "<<currentPageNum<<endl;
	cout<<"NextLeafPageNum: "<<NextLeafPageNum<<",  PageType: "<<PageType<<",  RcrdCount: "<<RcrdCount<<",  FreeSpace: "<<FreeSpace<<endl;
	cout<<"{"<<endl;


	int tmpRidPgNum = 0;
	int tmpRidSltNum = 0;
	if(PageType!=3){
		memcpy(&tmpPgNum, tmpPt, 4);
		cout<<"pgNum: "<<tmpPgNum<<endl;
		tmpPt += 4;
		if(attribute.type==TypeVarChar){
			int tmpKeyLen = 0;
			for(int i=0; i<RcrdCount; i++){
				memcpy(&tmpKeyLen, tmpPt, 4);	tmpPt += 4;
				memcpy(tmpKey, tmpPt, tmpKeyLen);	tmpPt += tmpKeyLen;
				memcpy(&tmpRidPgNum, tmpPt, 4);	tmpPt += 4;
				memcpy(&tmpRidSltNum, tmpPt, 4);	tmpPt += 4;
				memcpy(&tmpPgNum, tmpPt, 4);	tmpPt += 4;
				cout<<"Key: "<<(char*)tmpKey<<",  rid.pgNum: "<<tmpRidPgNum<<",  rid.sltNum: "<<tmpRidSltNum<<",  pgNum: "<<tmpPgNum<<endl;
			}
		}
		else{
			for(int i=0; i<RcrdCount; i++){
				memcpy(tmpKey, tmpPt, 4);	tmpPt += 4;
				memcpy(&tmpRidPgNum, tmpPt, 4);	tmpPt += 4;
				memcpy(&tmpRidSltNum, tmpPt, 4);	tmpPt += 4;
				memcpy(&tmpPgNum, tmpPt, 4);	tmpPt += 4;
				if(attribute.type==TypeInt)	cout<<"Key: "<<*((int*)tmpKey)<<",  rid.pgNum: "<<tmpRidPgNum<<",  rid.sltNum: "<<tmpRidSltNum<<",  pgNum: "<<tmpPgNum<<endl;
				else cout<<"Key: "<<*((float*)tmpKey)<<",  rid.pgNum: "<<tmpRidPgNum<<",  rid.sltNum: "<<tmpRidSltNum<<",  pgNum: "<<tmpPgNum<<endl;
			}
		}
	}
	else{
		if(attribute.type==TypeVarChar){
			int tmpKeyLen = 0;
			for(int i=0; i<RcrdCount; i++){
				memcpy(&tmpKeyLen, tmpPt, 4);	tmpPt += 4;
				memcpy(tmpKey, tmpPt, tmpKeyLen);	tmpPt += tmpKeyLen;
				memcpy(&tmpRidPgNum, tmpPt, 4);	tmpPt += 4;
				memcpy(&tmpRidSltNum, tmpPt, 4);	tmpPt += 4;
				cout<<"Key: "<<(char*)tmpKey<<",  rid.pgNum: "<<tmpRidPgNum<<",  rid.sltNum: "<<tmpRidSltNum<<endl;
			}
		}
		else{
			for(int i=0; i<RcrdCount; i++){
				memcpy(tmpKey, tmpPt, 4);	tmpPt += 4;
				memcpy(&tmpRidPgNum, tmpPt, 4);	tmpPt += 4;
				memcpy(&tmpRidSltNum, tmpPt, 4);	tmpPt += 4;
				if(attribute.type==TypeInt)	cout<<"Key: "<<*((int*)tmpKey)<<",  rid.pgNum: "<<tmpRidPgNum<<",  rid.sltNum: "<<tmpRidSltNum<<endl;
				else cout<<"Key: "<<*((float*)tmpKey)<<",  rid.pgNum: "<<tmpRidPgNum<<",  rid.sltNum: "<<tmpRidSltNum<<endl;
			}
		}
	}
	cout<<"}"<<endl;

	if(currentPageNum < ixfileHandle.fileHandle.getNumberOfPages() - 1){
		printPageContent(currentPageNum+1, ixfileHandle, attribute);
	}

	free(tmpKey);
	free(currentPage);
	return 0;
}

void IndexManager::prtKey( void* varValue,const Attribute &attribute, void* &preKey, void* currentKey, int offset, const RID &rid) const
{
	void* varSpace=malloc(PAGE_SIZE);
	int varLength=0;
	int* varP=new int;
	char varWord=0;
	char* varTemP=new char;

	if(attribute.type==TypeVarChar)
	{
		char* curKey=(char*)malloc(PAGE_SIZE);
		memset(curKey,0,PAGE_SIZE);
		int curKeyLength=0;
		memcpy(&curKeyLength,currentKey,sizeof(int));
		memcpy(curKey,(char*)currentKey+sizeof(int),curKeyLength);
		string cK=curKey;
		string pK;

		char* prKey=(char*)malloc(PAGE_SIZE);

		if(preKey==NULL){
			preKey = malloc(PAGE_SIZE);
			memset(preKey, 0, PAGE_SIZE);
			if(offset>0)
			{
				cout<<"]\"";
				cout<<",";
			}
			cout<<"\"";
			memcpy(varP,varValue,sizeof(int));
			varLength=*varP;
			memcpy(varSpace,(char*)varValue+sizeof(int),varLength);
			varTemP=(char*)varSpace;
			for(int i=0; i<varLength;i++)
			{
				varWord=*varTemP;
				cout<<varWord;
				varTemP++;
			}
			cout<<":[";
			cout<<"("<<rid.pageNum<<","<<rid.slotNum<<")";
		}
		else{
			memset(prKey,0,PAGE_SIZE);
			int prKeyLength=0;
			memcpy(&prKeyLength,preKey,sizeof(int));
			memcpy(prKey,(char*)preKey+sizeof(int),prKeyLength);
			string pK=prKey;

			if(cK != pK){
				if(offset>0)
				{
					cout<<"]\"";
					cout<<",";
				}
				cout<<"\"";
				memcpy(varP,varValue,sizeof(int));
				varLength=*varP;
				memcpy(varSpace,(char*)varValue+sizeof(int),varLength);
				varTemP=(char*)varSpace;
				for(int i=0; i<varLength;i++)
				{
					varWord=*varTemP;
					cout<<varWord;
					varTemP++;
				}
				cout<<":[";
				cout<<"("<<rid.pageNum<<","<<rid.slotNum<<")";
			}
			else{
				cout<<",";
				cout<<"("<<rid.pageNum<<","<<rid.slotNum<<")";
			}
		}

		free(prKey);
		free(varSpace);
		free(curKey);
		return;
	}
	else if(attribute.type==TypeInt)
	{
		if(preKey==NULL || (*(int*)currentKey != *(int*)preKey) )
		{

			if(preKey==NULL){
				preKey = malloc(PAGE_SIZE);
				memset(preKey, 0, PAGE_SIZE);
			}
			if(offset>0)
			{
				cout<<"]\"";
				cout<<",";
			}
			cout<<"\"";
			cout<<*(int*)varValue;
			cout<<":[";
			cout<<"("<<rid.pageNum<<","<<rid.slotNum<<")";
		}
		else
		{
			cout<<",";
			cout<<"("<<rid.pageNum<<","<<rid.slotNum<<")";
		}
		free(varSpace);
		return;
	}
	else if(attribute.type==TypeReal)
	{
		if( (preKey==NULL) || (*(float*)currentKey != *(float*)preKey))
		{

			if(preKey==NULL){
				preKey = malloc(PAGE_SIZE);
				memset(preKey, 0, PAGE_SIZE);
			}

			if(offset>0)
			{
				cout<<"]\"";
				cout<<",";
			}
			cout<<"\"";
			cout<<*(float*)varValue;
			cout<<":[";
			cout<<"("<<rid.pageNum<<","<<rid.slotNum<<")";
		}
		else
		{
			cout<<",";
			cout<<"("<<rid.pageNum<<","<<rid.slotNum<<")";
		}

		free(varSpace);
		return;
	}
return;
}
void IndexManager::prtKey( void* varValue,const Attribute &attribute) const
{
	void* varSpace=malloc(PAGE_SIZE);
	int varLength=0;
	int* varP=new int;
	char varWord=0;
	char* varTemP=new char;

	if(attribute.type==TypeVarChar)
	{
		memcpy(varP,varValue,sizeof(int));
		varLength=*varP;
		memcpy(varSpace,(char*)varValue+sizeof(int),varLength);
		varTemP=(char*)varSpace;
		for(int i=0; i<varLength;i++)
		{
			varWord=*varTemP;
			cout<<varWord;
			varTemP++;
		}
		free(varSpace);
		return;
	}
	else if(attribute.type==TypeInt)
	{
		cout<<*(int*)varValue;
		free(varSpace);
		return;
	}
	else if(attribute.type==TypeReal)
	{
		cout<<*(float*)varValue;
		free(varSpace);
		return;
	}
return;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const
{
	int layer=0;
	void* page=malloc(PAGE_SIZE);
	//pageNumber rootPage=ixfileHandle.getRootPageNum();
	ixfileHandle.readPage(0,page);
	printBtreeNode(ixfileHandle, attribute,layer,0);
	cout<<endl;
	free(page);
	return;
}
void IndexManager::printBtreeNode(IXFileHandle &ixfileHandle, const Attribute &attribute,int layer,pageNumber nodeNumber) const
{
	void* page=malloc(PAGE_SIZE);
	//pageNumber rootPage=ixfileHandle.getRootPageNum();
	nodeDescriptor nodeDesc;
	int pageNum_left=0;
	int pageNum_right=0;
	int rootKey=0;
	int interKey=0;
	int leafKey=0;
	int numOfRoot=0;
	int numOfPageNumInRootPage=0;
	int offset=0;
	int* varKeyP=new int;
	int varTotLength=0;
	void* varValue=malloc(PAGE_SIZE);
	void* intValue=malloc(PAGE_SIZE);
	void* realValue=malloc(PAGE_SIZE);
	int* intP = new int;
	float* realP = new float;
	RID rid;
	rid.pageNum=0;
	rid.slotNum=0;
	int* tempP;


	ixfileHandle.readPage(nodeNumber,page);
	memcpy(&nodeDesc,(char*)page+PAGE_SIZE-16,sizeof(nodeDescriptor));
/*	cout<<"size of nodeDescriptor: "<<sizeof(nodeDescriptor)<<endl;
	cout<<"freespace: "<<nodeDesc.freeSpace<<endl;
	cout<<"numofkey: "<<nodeDesc.numOfKey<<endl;
	cout<<"nodeType: "<<nodeDesc.nodeType<<endl;
	cout<<"next: "<<nodeDesc.nextSilbingPageNum<<endl;
*/
	/*for(int q=0;q<layer;q++)
	{
		cout<<"\t";

	}
	*/
	//numOfPageNumInRootPage=nodeDesc.numOfKey+1;


	if(attribute.type==TypeVarChar)
	{
		for(int j=0;j<layer;j++)
		{
			cout<<"\t";
		}
		cout<<"{";
		if(nodeDesc.nodeType==NONLEAF||nodeDesc.nodeType==2)
		{
			vector<pageNumber> link;
			cout<<"\"key\":[";
			while(offset<(PAGE_SIZE-nodeDesc.freeSpace-20))
			{
				if(offset>0)
				{
					cout<<",";
				}
				memcpy(varKeyP,(char*)page+offset+sizeof(int),sizeof(int));
				varTotLength=4+*varKeyP;
				memset(varValue, 0, PAGE_SIZE);
				memcpy(varValue,(char*)page+offset+sizeof(int),varTotLength);
				cout<<"\"";
				prtKey(varValue,attribute);
				cout<<"\"";
				memcpy(&pageNum_left,(char*)page+offset,sizeof(int));
				memcpy(&pageNum_right,(char*)page+offset+sizeof(int)+varTotLength+8,sizeof(int));
				link.push_back(pageNum_left);
				//free(varValue);
				offset+=sizeof(int)+varTotLength+8;


			}
			cout<<"],\n";
			link.push_back(pageNum_right);

			for(int x=0;x<layer;x++)
			{
				cout<<"\t";
			}
			cout<<"\"children\":["<<endl;
			for(int v=0;v<link.size();v++)
			{
				printBtreeNode(ixfileHandle,attribute,layer+1,link[v]);
				if(v<link.size()-1)
				{
					cout<<","<<endl;
				}
			}
			cout<<endl;
			cout<<"]}";

			free(varValue);
			free(intValue);
			free(realValue);
			free(page);
			return;
		}

		else if(nodeDesc.nodeType==LEAF)
		{
			cout<<"\"key\":[";
			offset=0;
			//void* preKey=malloc(PAGE_SIZE);
			//memset(preKey,0,PAGE_SIZE);
			void* preKey = NULL;
			void* currentKey=malloc(PAGE_SIZE);
			memset(currentKey,0,PAGE_SIZE);
			while(offset<(PAGE_SIZE-nodeDesc.freeSpace-20))
			{

				memcpy(varKeyP,(char*)page+offset,sizeof(int));
				varTotLength=4+*varKeyP;
				memset(varValue, 0, PAGE_SIZE);
				memcpy(varValue,(char*)page+offset,varTotLength);
				memcpy(currentKey,(char*)page+offset,varTotLength);
				memcpy(&rid,(char*)page+offset+varTotLength,sizeof(RID));
				prtKey(varValue,attribute,preKey,currentKey,offset,rid);
				memcpy(preKey,currentKey,varTotLength);

				//free(varValue);
				offset+=varTotLength+sizeof(RID);
			}
			cout<<"]\"]}";


			free(preKey);
			free(currentKey);

			free(varValue);
			free(intValue);
			free(realValue);
			free(page);
			return;
		}

	}

	else if(attribute.type==TypeInt)
	{
		for(int j=0;j<layer;j++)
		{
			cout<<"\t";
		}
		cout<<"{";
		if(nodeDesc.nodeType==NONLEAF||nodeDesc.nodeType==2)
		{
			vector<pageNumber> link;
			cout<<"\"key\":[";
			while(offset<(PAGE_SIZE-nodeDesc.freeSpace-20))
			{
				if(offset>0)
				{
					cout<<",";
				}
				//tempP=(int*)((char*)page+offset+sizeof(int));
				//cout<<"tempP: "<<tempP<<endl;
				memcpy(intP,(char*)page+offset+sizeof(int),sizeof(int));

				memset(intValue, 0, PAGE_SIZE);
				memcpy(intValue,(char*)page+offset+sizeof(int),sizeof(int));
				cout<<"\"";
				prtKey(intValue,attribute);
				cout<<"\"";
				memcpy(&pageNum_left,(char*)page+offset,sizeof(int));
				memcpy(&pageNum_right,(char*)page+offset+sizeof(int)+sizeof(int)+8,sizeof(int));
				link.push_back(pageNum_left);
				//free(intValue);
				offset+=sizeof(int)+sizeof(int)+8;


			}
			cout<<"],\n";
			link.push_back(pageNum_right);

			for(int x=0;x<layer;x++)
			{
				cout<<"\t";
			}
			cout<<"\"children\":["<<endl;
			for(int v=0;v<link.size();v++)
			{
				printBtreeNode(ixfileHandle,attribute,layer+1,link[v]);
				if(v<link.size()-1)
				{
					cout<<","<<endl;
				}
			}
			cout<<endl;
			cout<<"]}";


			free(varValue);
			free(intValue);
			free(realValue);
			free(page);
			return;
		}

		else if(nodeDesc.nodeType==LEAF)
		{
			cout<<"\"key\":[";
			offset=0;
			//void* preKey=malloc(sizeof(int));
			//memset(preKey,0,sizeof(int));
			void* preKey = NULL;
			void* currentKey=malloc(sizeof(int));
			memset(currentKey,0,sizeof(int));
			while(offset<(PAGE_SIZE-nodeDesc.freeSpace-20))
			{

				memcpy(intP,(char*)page+offset,sizeof(int));

				memset(intValue, 0, PAGE_SIZE);
				memcpy(intValue,(char*)page+offset,sizeof(int));
				memcpy(currentKey,(char*)page+offset,sizeof(int));
				memcpy(&rid,(char*)page+offset+sizeof(int),sizeof(RID));
				prtKey(intValue,attribute,preKey,currentKey,offset,rid);
				memcpy(preKey,currentKey,sizeof(int));

				//free(intValue);
				offset+=sizeof(int)+sizeof(RID);
			}
			cout<<"]\"]}";

			free(preKey);
			free(currentKey);


			free(varValue);
			free(intValue);
			free(realValue);
			free(page);
			return;
		}

	}
	else if(attribute.type==TypeReal)
	{
		for(int j=0;j<layer;j++)
		{
			cout<<"\t";
		}
		cout<<"{";
		if(nodeDesc.nodeType==NONLEAF||nodeDesc.nodeType==2)
		{
			vector<pageNumber> link;
			cout<<"\"key\":[";
			while(offset<(PAGE_SIZE-nodeDesc.freeSpace-20))
			{
				if(offset>0)
				{
					cout<<",";
				}
				memcpy(realP,(char*)page+offset+sizeof(int),sizeof(float));

				memset(realValue, 0, PAGE_SIZE);
				memcpy(realValue,(char*)page+offset+sizeof(int),sizeof(float));
				cout<<"\"";
				prtKey(realValue,attribute);
				cout<<"\"";
				memcpy(&pageNum_left,(char*)page+offset,sizeof(int));
				memcpy(&pageNum_right,(char*)page+offset+sizeof(float)+sizeof(int)+8,sizeof(int));
				link.push_back(pageNum_left);
				//free(realValue);
				offset+=sizeof(float)+sizeof(int)+8;


			}
			cout<<"],\n";
			link.push_back(pageNum_right);

			for(int x=0;x<layer;x++)
			{
				cout<<"\t";
			}
			cout<<"\"children\":["<<endl;
			for(int v=0;v<link.size();v++)
			{
				printBtreeNode(ixfileHandle,attribute,layer+1,link[v]);
				if(v<link.size()-1)
				{
					cout<<","<<endl;
				}
			}
			cout<<endl;
			cout<<"]}";

			free(varValue);
			free(intValue);
			free(realValue);
			free(page);
			return;
		}

		else if(nodeDesc.nodeType==LEAF)
		{
			cout<<"\"key\":[";
			//void* preKey=malloc(sizeof(float));
			//memset(preKey,0,sizeof(float));
			void* preKey = NULL;
			void* currentKey=malloc(sizeof(float));
			memset(currentKey,0,sizeof(float));

			offset=0;
			while(offset<(PAGE_SIZE-nodeDesc.freeSpace-20))
			{

				memcpy(realP,(char*)page+offset,sizeof(float));

				memset(realValue, 0, PAGE_SIZE);
				memcpy(realValue,(char*)page+offset,sizeof(float));
				memcpy(currentKey,(char*)page+offset,sizeof(float));
				memcpy(&rid,(char*)page+offset+sizeof(float),sizeof(RID));
				prtKey(realValue,attribute,preKey,currentKey,offset,rid);
				memcpy(preKey,currentKey,sizeof(float));

				//free(realValue);
				offset+=sizeof(int)+sizeof(RID);
			}
			cout<<"]\"]}";

			free(preKey);
			free(currentKey);

			free(varValue);
			free(intValue);
			free(realValue);
			free(page);
			return;
		}


	}
}
RC IXFileHandle::readPage(pageNumber pageNum, void* data)
{
	this->ixReadPageCounter++;
	return fileHandle.readPage(pageNum,data);
}

RC IXFileHandle::writePage(pageNumber pageNum, void* data)
{
	this->ixWritePageCounter++;
	return fileHandle.writePage(pageNum,data);
}

unsigned int IXFileHandle::getRootPageNum()
{
	//defines

	void* page=malloc(PAGE_SIZE);
	memset(page,0,PAGE_SIZE);
	int dSize=PAGE_SIZE/4;
	int freeSpace=0;
	int numOfKey=0;
	int pageType=-1;
	int nextSiplingPageNum=0;
	pageNumber rootPageNum=1;    //assume the page number of root is 1
	//void* tmpP=0;
	RC rc=0;
	int empty=0;

	if(readPage(0,page)==-1)
	{
		rootPageNum=1;
		freeSpace=PAGE_SIZE-12-16;
		numOfKey=1;
		pageType=LEAF;
		nextSiplingPageNum=0;
		memcpy((char*)page+sizeof(pageNumber),&rootPageNum,sizeof(pageNumber));
		memcpy((char*)page+PAGE_SIZE-sizeof(int),&freeSpace,sizeof(int));
		memcpy((char*)page+PAGE_SIZE-sizeof(int)-sizeof(int),&numOfKey,sizeof(int));
		memcpy((char*)page+PAGE_SIZE-sizeof(int)-sizeof(int)-sizeof(int),&pageType,sizeof(int));
		memcpy((char*)page+PAGE_SIZE-sizeof(int)-sizeof(int)-sizeof(int)-sizeof(int),&nextSiplingPageNum,sizeof(int));
		rc=writePage(0,page);
	}
	else
	{
		memcpy(&rootPageNum,(char*)page+sizeof(pageNumber),sizeof(int));
		return rootPageNum;
	}
	free(page);
	return rootPageNum;
}

int IndexManager::getLenOfKey(const Attribute &attribute, const void *KeyPt){
	if(attribute.type==TypeVarChar){
		int tmpLenKey = 0;
		memcpy(&tmpLenKey, (char*)KeyPt, 4);
		return (tmpLenKey + 4);

	}
	else if(attribute.type==TypeInt || attribute.type==TypeReal){
		return 4;
	}
	return 4;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void *lowKey,
        const void *highKey,
        bool lowKeyInclusive,
        bool highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{

	void* rootPage = malloc(PAGE_SIZE);
	int targetPageNum = 0;
	int targetOffset = 0;
	stack<treeNode> parentStkList;

	if(ixfileHandle.fileHandle.readPage(0, rootPage)==-1) return -1;

	//ix_ScanIterator.ixfileHandlePt = new IXFileHandle;
	// *(ix_ScanIterator.ixfileHandlePt) = ixfileHandle;
	openFile(ixfileHandle.fileHandle.fileName, *(ix_ScanIterator.ixfileHandlePt));

	//ix_ScanIterator.attributePt = new Attribute;
	*(ix_ScanIterator.attributePt) = attribute;

	ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
	ix_ScanIterator.highKeyInclusive = highKeyInclusive;

	ix_ScanIterator.lowKey = malloc(PAGE_SIZE);
	memset(ix_ScanIterator.lowKey, 0, PAGE_SIZE);

	ix_ScanIterator.highKey = malloc(PAGE_SIZE);
	memset(ix_ScanIterator.highKey, 0, PAGE_SIZE);

	if(lowKey==NULL){
		ix_ScanIterator.lowKey=NULL;
	}
	else{
		int lenOfKey = getLenOfKey(attribute, lowKey);
		memcpy(ix_ScanIterator.lowKey, lowKey, lenOfKey);
	}

	if(highKey==NULL){
		ix_ScanIterator.highKey=NULL;
	}
	else{
		int lenOfKey = getLenOfKey(attribute, highKey);
		memcpy(ix_ScanIterator.highKey, highKey, lenOfKey);
	}


	if(lowKeyInclusive==true){
		ix_ScanIterator.lowKeyRidPgNum = INT_MIN;  // fxck unsigned!
		ix_ScanIterator.lowKeyRidSltNum = INT_MIN;  //
	}
	else{
		ix_ScanIterator.lowKeyRidPgNum = INT_MAX;
		ix_ScanIterator.lowKeyRidSltNum = INT_MAX;
	}


	if(highKeyInclusive==true){
		ix_ScanIterator.highKeyRidPgNum = INT_MAX;
		ix_ScanIterator.highKeyRidSltNum = INT_MAX;
	}
	else{
		ix_ScanIterator.highKeyRidPgNum = -1;
		ix_ScanIterator.highKeyRidSltNum = -1;
	}

	findLeafPos(ixfileHandle, attribute, lowKey, ix_ScanIterator.lowKeyRidPgNum,
			ix_ScanIterator.lowKeyRidSltNum, rootPage, targetPageNum, targetOffset, parentStkList);

	ix_ScanIterator.currentPageNum = targetPageNum;
	ix_ScanIterator.currentOffset = targetOffset;

	ix_ScanIterator.lastKey = malloc(PAGE_SIZE);
	memset(ix_ScanIterator.lastKey, 0, PAGE_SIZE);
	ix_ScanIterator.lastPageNum = -1;
	ix_ScanIterator.lastSlotNum = -1;


	unsigned readPageCount,  appendPageCount;
	ixfileHandle.collectCounterValues(readPageCount, ix_ScanIterator.lastWritePageCount, appendPageCount);

	ix_ScanIterator.lastLenWholeKey = 0;
	free(rootPage);
    return 0;
}

IX_ScanIterator::IX_ScanIterator()
{
    ixfileHandlePt = new IXFileHandle;
    attributePt = new Attribute;
    lowKey = NULL;
    highKey = NULL;
    lastKey = NULL;
    lastPageNum = -1;
    lastSlotNum = -1;
    lowKeyInclusive = false;
    highKeyInclusive = false;
    highKeyRidPgNum = 0;
    highKeyRidSltNum = 0;
    lowKeyRidPgNum = 0;
    lowKeyRidSltNum = 0;
    currentPageNum = 0;
    currentOffset = 0;
    lastWritePageCount = 0;
    lastLenWholeKey = 0;
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{


	// if currentPage has key available
	unsigned readPageCount, writePageCount, appendPageCount;
	ixfileHandlePt->collectCounterValues(readPageCount, writePageCount, appendPageCount);
	if(writePageCount!= lastWritePageCount){

		// Only for one delete at current rid:

		lastWritePageCount = writePageCount;
		if(currentOffset>0) currentOffset -= lastLenWholeKey + 4 + 4;
		return getNextEntry(rid, key);

		// for a general edit case: findLeafPos(lastKey) ? or Scan(... lastKey, false, ...)

	}
	else
	{

		void* currentPage = malloc(PAGE_SIZE);
		if(currentPageNum<0) return  IX_EOF;
		if(ixfileHandlePt->fileHandle.readPage(currentPageNum, currentPage) == -1) return IX_EOF;

		int nextLeafPageNum = -1;
		int pageType = -1;
		int rcrdCount = -1;
		int freeSpace = -1;
		IndexManager::readPageInfo(currentPage, nextLeafPageNum, pageType, rcrdCount, freeSpace);

		// if no available key in this page, try next Page

		if(rcrdCount == 0 ){
			currentPageNum = nextLeafPageNum;
			currentOffset = 0;
			return (getNextEntry(rid, key));
		}

		int lenWholeKey; // not include rid.pageNum rid.slotNum
		if(attributePt->type==TypeVarChar){
			int tmpLenKey = 0;
			memcpy(&tmpLenKey, (char*)currentPage + currentOffset, 4);
			memcpy(key, (char*)currentPage + currentOffset, 4+ tmpLenKey); // whole key include 4Byte length
			memcpy(&(rid.pageNum), (char*)currentPage + currentOffset + 4 + tmpLenKey, 4);
			memcpy(&(rid.slotNum), (char*)currentPage + currentOffset + 4 + tmpLenKey + 4, 4);
			lenWholeKey = tmpLenKey + 4;
		}
		else if(attributePt->type==TypeInt || attributePt->type==TypeReal){
			memcpy(key, (char*)currentPage + currentOffset, 4);
			memcpy(&(rid.pageNum), (char*)currentPage + currentOffset + 4, 4);
			memcpy(&(rid.slotNum), (char*)currentPage + currentOffset + 4 + 4, 4);
			lenWholeKey = 4;
		}

		lastLenWholeKey = lenWholeKey;
		// compare with highKey
		int tmpFlag = IndexManager::compareKey(*attributePt, key, rid.pageNum, rid.slotNum, highKey, highKeyRidPgNum, highKeyRidSltNum);
		// if this key is qualified by highKey
		if( tmpFlag == -1 || tmpFlag == 4){
			if(attributePt->type==TypeVarChar){
				int tmpLenKey = 0;
				memcpy(&tmpLenKey, (char*)key, 4);
				lenWholeKey = tmpLenKey + 4;
			}
			else{
				lenWholeKey = 4;
			}
			memset(lastKey, 0, PAGE_SIZE);
			memcpy(lastKey, key, lenWholeKey);
			lastPageNum = rid.pageNum;
			lastSlotNum = rid.slotNum;

			currentOffset += lenWholeKey + 4 + 4;
			if(currentOffset >= PAGE_SIZE - 16 - freeSpace){
				currentPageNum = nextLeafPageNum;
				currentOffset = 0;
			}
			free(currentPage);
			return 0;
		}
		else{
			free(currentPage);
			return IX_EOF; // out of maxKey, terminate
		}

	}

}

RC IX_ScanIterator::close()
{
    ixfileHandlePt = NULL;
    attributePt = NULL;
    free(lowKey);
    lowKey = NULL;
    free(highKey);
    highKey = NULL;
    lowKeyInclusive = false;
    highKeyInclusive = false;
    highKeyRidPgNum = 0;
    highKeyRidSltNum = 0;
    lowKeyRidPgNum = 0;
    lowKeyRidSltNum = 0;
    currentPageNum = 0;
    currentOffset = 0;

    free(lastKey);
    lastKey = NULL;
    lastPageNum = -1;
    lastSlotNum = -1;

    return 0;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	return (this->fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount));
}
