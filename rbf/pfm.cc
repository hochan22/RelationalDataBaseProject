#include "pfm.h"
#include<fstream>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<iostream>
#include<set>
using namespace std;

PagedFileManager* PagedFileManager::_pf_manager = 0;
set<string> PagedFileManager:: openedFileList;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
	FILE* tmpFile = fopen(fileName.c_str(), "r");
	if(tmpFile==NULL){
		tmpFile = fopen(fileName.c_str(), "w");

	    void* emptyPage = malloc(PAGE_SIZE); // Create hidden page to store (readPageCounter, writePageCounter, appendPageCounter) for file
	    memset(emptyPage, 0, PAGE_SIZE);

	    FileHandle fileHandle;
		fileHandle.setPf(tmpFile);
		fileHandle.appendPage(emptyPage);
		closeFile(fileHandle);

		free(emptyPage);
		return 0;
	}
	else return -1;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
	if(openedFileList.count(fileName)!=0){
		openedFileList.erase(fileName);
	}

	return remove(fileName.c_str());
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{


	FILE* tmpFile = fopen(fileName.c_str(), "r+");
	if(tmpFile==NULL) return -1;
	else{
		int returnFlag = 0;
		if(openedFileList.count(fileName)!=0) returnFlag = -1;
		else returnFlag = 0;

		openedFileList.insert(fileName);

		fileHandle.fileState = 1;
		fileHandle.fileName = fileName;
		fileHandle.setPf(tmpFile);
		fileHandle.initialCnt();

		return returnFlag;
	}
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	void* emptyData = malloc(PAGE_SIZE);
	void* data = emptyData;
	memset(data, 0, PAGE_SIZE);
	memcpy(data, &(fileHandle.readPageCounter), sizeof(int));
    data = (char*)data + sizeof(int);
    memcpy(data, &(fileHandle.writePageCounter), sizeof(int));
    data = (char*)data + sizeof(int);
    memcpy(data, &(fileHandle.appendPageCounter), sizeof(int));
	fwrite(emptyData, 1, PAGE_SIZE, fileHandle.fileHandlePf);
	fflush(fileHandle.fileHandlePf);
	free(emptyData);

	if(fclose(fileHandle.fileHandlePf)==0){
		openedFileList.erase(fileHandle.fileName);
		fileHandle.fileState = 0;
		return 0;
	}
	else return -1;
}







FileHandle::FileHandle()
{
	// these values are stored at the hidden first page of file
	// from outside, the available pageNum still starts from 0.

	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter= 0;
	fileHandlePf = 0;

	fileState = 0;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::initialCnt(){

	fileState = 0;
	void* emptyData = malloc(PAGE_SIZE);
	void* data = emptyData;
	fread(data, 1, PAGE_SIZE, fileHandlePf);

    memcpy(&readPageCounter, (int*)data, sizeof(int));
    data = (char*)data + sizeof(int);
    memcpy(&writePageCounter, (int*)data, sizeof(int));
    data = (char*)data + sizeof(int);
    memcpy(&appendPageCounter, (int*)data, sizeof(int));

    free(emptyData);
	return 0;
}

RC FileHandle::readPage(PageNum pageNum, void *data)
{

	//if(fileState==-1) return -1;
	if( pageNum +1 > getNumberOfPages() || pageNum +1 == 0) {
		return -1;
	}
	else{
		fseek(fileHandlePf, (pageNum+1)*PAGE_SIZE, 0 );
		fread(data, 1, PAGE_SIZE, fileHandlePf);
		readPageCounter++;
	    rewind(fileHandlePf);
		return 0;
	}
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	//if(fileState==-1) return -1;
	//if( (long long int)pageNum > (int)((int)getNumberOfPages()) ) {
	//    return -1;
	//}
	//else{
		fseek(fileHandlePf, (pageNum+1)*PAGE_SIZE, 0 );
		fwrite(data, 1, PAGE_SIZE, fileHandlePf);
		writePageCounter++;
	    rewind(fileHandlePf);
		return 0;
	//}
}


RC FileHandle::appendPage(const void *data)
{
	//if(fileState==-1) return -1;
	fseek(fileHandlePf, 0, SEEK_END);
	fwrite(data, 1, PAGE_SIZE, fileHandlePf);
    appendPageCounter ++;
    rewind(fileHandlePf);
    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
	//if(fileState==-1) return 0;
	if(PagedFileManager::openedFileList.count(fileName)==0){
		FILE* tmpFile = fopen(fileName.c_str(), "r+");
		if(tmpFile==NULL) return 0;
	}

    fseek(fileHandlePf, 0, SEEK_END);
    long long fileSizeByte = ftell(fileHandlePf);
    rewind(fileHandlePf);
    return fileSizeByte / PAGE_SIZE - 1;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = readPageCounter;
	writePageCount = writePageCounter;
	appendPageCount = appendPageCounter;
    return 0;
}

RC FileHandle::setPf(FILE* pt)
{
	fileHandlePf = pt;
	return 0;
}

RC FileHandle::forceFflush(){
	fflush(fileHandlePf);
	return 0;
}
