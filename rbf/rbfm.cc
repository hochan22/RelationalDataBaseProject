#include "rbfm.h"
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
/////////////////
#include <bitset>
/////////////////
using namespace std;

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager){
        _rbf_manager = new RecordBasedFileManager();
    }

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    rbf_pfm = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	return(rbf_pfm->createFile(fileName));
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return(rbf_pfm->destroyFile(fileName));
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return(rbf_pfm->openFile(fileName, fileHandle));
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return(rbf_pfm->closeFile(fileHandle));
}


// given a insert-format void* data, calculate the record length as the stored-in-file format
int RecordBasedFileManager::lengthOfRecordStored(const vector<Attribute> &recordDescriptor, const void* data){
	int cntAttribute = recordDescriptor.size();
	int lenRcrdStd = 2 + 2 * cntAttribute; // first 2 Byte for "cnt of fields", followed by every 2 Byte for an offset of each field

	//int cntAttribute = recordDescriptor.size();
	int nullFieldsByteCnt =  ceil((double)cntAttribute / 8) ;
	char* tmpNullByte = new char(0);
	int sizeOfField_storage = 0;
	int* lenVarChar = new int(0);
	void* dataPt = (char*)data + nullFieldsByteCnt;
	void* tmpValue = malloc(PAGE_SIZE);

	for(int i=0; i<cntAttribute; i++){
			memcpy(tmpNullByte, (char*)data + i/8, 1);
			// non-null field
			if( ( (*tmpNullByte>>(7-i%8)) & 1 ) == 0 ){
				if(recordDescriptor[i].type == TypeVarChar){
					memcpy(lenVarChar, dataPt, sizeof(int));
					sizeOfField_storage = *lenVarChar + sizeof(int);
				}
				else{
					sizeOfField_storage = recordDescriptor[i].length;
				}

				memset(tmpValue, 0, PAGE_SIZE);
				memcpy(tmpValue, dataPt, sizeOfField_storage);
				dataPt = (char*)dataPt + sizeOfField_storage;

				//cout<<sizeOfField_storage<<endl;

				lenRcrdStd += sizeOfField_storage;
			}

	}

	free(tmpValue);
	free(lenVarChar);
	free(tmpNullByte);
    return lenRcrdStd;
}

// write the record to (newPage, offsetRecord ) position, only record, no slot, F, N information changed.
RC RecordBasedFileManager::AddRecordInNewpage(void* newPage, int offsetRecord, const vector<Attribute> &recordDescriptor, const void *data){
	char* tmpPt = (char*)newPage + offsetRecord;
	int cntAttri = recordDescriptor.size();
	int tmpOffset = 2+2*cntAttri; // to record the end_offset of each field (precisely, the beginning of the next field) with the beginning of the record as basis
	// Thus to read field_i is [ Pt_recordBeginning + 2+2*cntAttri + offset(i-1) : Pt_recordBeginning + 2+2*cntAttri + offset(i)-1 ]



	// Save Pre-data info in the newPage [ cnt_of_field, offset1, offset2... offsetn ]
	//char* tmpNullByte = new char(0);

	memcpy(tmpPt, &cntAttri, 2);
	tmpPt += 2;
	int nullFlag = 0;
	int nullFlagAndOffset = 0;
	//int tmplen = 0;



	int cntAttribute = recordDescriptor.size();
	int nullFieldsByteCnt =  ceil((double)cntAttribute / 8) ;
	char* tmpNullByte = new char(0);
	int sizeOfField_storage = 0;
	int* lenVarChar = new int(0);
	void* dataPt = (char*)data + nullFieldsByteCnt;
	//void* tmpValue = malloc(PAGE_SIZE);



	for(int i=0; i<cntAttri; i++){

		memcpy(tmpNullByte, (char*)data + i/8, 1);
		// non-null field
		if( ( (*tmpNullByte>>(7-i%8)) & 1 ) == 0 ){
			nullFlag = 0;
			if(recordDescriptor[i].type == TypeVarChar){
				memcpy(lenVarChar, dataPt, sizeof(int));
				sizeOfField_storage = *lenVarChar + sizeof(int);
			}
			else{
				sizeOfField_storage = recordDescriptor[i].length;
			}


			dataPt = (char*)dataPt + sizeOfField_storage;
		}
		else{
			nullFlag = 1;
			sizeOfField_storage = 0;
		}



		tmpOffset += sizeOfField_storage;
		//if(recordDescriptor[i].type == TypeVarChar) tmpOffset += sizeof(int); //?
		nullFlagAndOffset = (nullFlag<<15) | tmpOffset;
		memcpy(tmpPt, &nullFlagAndOffset, 2);  // 100..Offset (2Bytes length) means NULL; 000...Offset (2Bytes length) means Non-null.
		tmpPt += 2;


		/////////////////////////////////
		//cout<<i<<" "<<nullFlag<<"  tmpOffest "<<tmpOffset<<"  nullFlagAndOffset "<<(bitset<2*8>(nullFlagAndOffset))<<endl;
		/////////////////////////////////
	}


	// Save real field data in the newPage [field1, field2 ... fieldn]

	dataPt = (char*)data + nullFieldsByteCnt;
	memset(tmpNullByte, 0, sizeof(char));

	for(int i=0; i<cntAttri; i++){
		memcpy(tmpNullByte, (char*)data + i/8, 1);
		// non-null field
		if( ( (*tmpNullByte>>(7-i%8)) & 1 ) == 0 ){
			if(recordDescriptor[i].type == TypeVarChar){
				memcpy(lenVarChar, dataPt, sizeof(int));
				sizeOfField_storage = *lenVarChar + sizeof(int); // VarChar length = sizeof(int) + lenVarChar
			}
			else{
				sizeOfField_storage = recordDescriptor[i].length;
			}

			//memset(tmpValue, 0, PAGE_SIZE);
			//memcpy(tmpValue, dataPt, sizeOfField_storage);


		//	if(recordDescriptor[i].type == TypeReal) cout<<*(float*)tmpValue<<endl;
		//	else if(recordDescriptor[i].type == TypeInt) cout<<*(int*)tmpValue<<endl;

			memcpy(tmpPt, dataPt, sizeOfField_storage);

			tmpPt += sizeOfField_storage;
			dataPt = (char*)dataPt + sizeOfField_storage;

		}

	}



	delete(tmpNullByte);
	delete(lenVarChar);

	return 0;
}

// write slotInfo, N, F (assigned value) to the page
RC RecordBasedFileManager::UpdateDirectInNewPage(void* newPage, int offsetRecord, int offsetSlotDirect, int lenRcrdStd, int tmpNewN, int tmpNewF){
	char* tmpPt = (char*)newPage + offsetSlotDirect;
	memcpy(tmpPt, &offsetRecord, 2);
	tmpPt += 2;
	memcpy(tmpPt, &lenRcrdStd, 2);

	///////////////////////////////////////////
	//cout<<"During Update FN, N= "<<N<<"  newN= "<<tmpNewN<<"   F="<<F<<"  tmpNewF="<<tmpNewF<<endl;
	///////////////////////////////////////////
	tmpPt = (char*)newPage + PAGE_SIZE - 4;
	memcpy(tmpPt, &tmpNewN, 2);
	tmpPt += 2;
	memcpy(tmpPt, &tmpNewF, 2);



	return 0;
}

// find space and insert the record, change slotInfo, F, N
RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {

	////////////////////////////////////
	//printRecord(recordDescriptor, data);
	////////////////////////////////////


	void* newPage = malloc(PAGE_SIZE);
    memset(newPage, 0, PAGE_SIZE);
    int tryPageNum = 0;
    int N_slotInPage = 0;
    int F_freeByteInPage = 0;
    bool flag_FreeSpaceFound = false;
    int lenRcrdStd = lengthOfRecordStored(recordDescriptor, data);
    int pageNum = fileHandle.getNumberOfPages();
    int offsetRecord = 0;
    int offsetSlotDirect = 0;
    char* tmpPt = 0;

    int tmpNewN = 0;
    int tmpNewF = 0;

    void* emptyPage = malloc(PAGE_SIZE);
    memset(emptyPage, 0, PAGE_SIZE);

    for(tryPageNum = pageNum-1; tryPageNum >=0 ; tryPageNum--){
    	N_slotInPage = 0;
    	F_freeByteInPage = 0;
    	fileHandle.readPage(tryPageNum, newPage);
		tmpPt = (char*)newPage + PAGE_SIZE - 4;
		memcpy(&N_slotInPage, tmpPt, 2);
		tmpPt = (char*)tmpPt + 2;
		memcpy(&F_freeByteInPage, tmpPt, 2);

		/////////////////////////////////////////
		//cout<<"tryPageNum= "<<tryPageNum<<"  N= "<<N_slotInPage<<"  F= "<<F_freeByteInPage<<"  lenRcrdStd + 2 = "<<lenRcrdStd<<endl;
		/////////////////////////////////////////

		if(F_freeByteInPage >= lenRcrdStd + 4){
			flag_FreeSpaceFound = true; // These N,F operation is done in UpdateDirectInNewPage();

			//reuse slot with tombstone
			char* tmpSlotPt = (char*)newPage + PAGE_SIZE - 8;
			int tmpSlotRcrdOffset = 0;
			int tmpSlotNum = 0;
			memcpy(&tmpSlotRcrdOffset, tmpSlotPt, 2);
			while((tmpSlotRcrdOffset>>15) != 1 && tmpSlotNum < N_slotInPage){
				tmpSlotPt -= 4;
				tmpSlotNum++;
				tmpSlotRcrdOffset = 0;
				memcpy(&tmpSlotRcrdOffset, tmpSlotPt, 2);
			}


			if(tmpSlotNum < N_slotInPage){

				//cout<<"reuse! ";

				tmpNewN = N_slotInPage;
				tmpNewF = F_freeByteInPage - lenRcrdStd;

			}
			else{

				//cout<<"add at end. ";

				tmpNewN = N_slotInPage + 1;
				tmpNewF = F_freeByteInPage - lenRcrdStd - 4;
			}

			offsetRecord = PAGE_SIZE - (2 + 2 + 4*N_slotInPage + F_freeByteInPage);
			offsetSlotDirect = PAGE_SIZE - (2 + 2 + 4*tmpSlotNum) - 4;

			rid.slotNum = tmpSlotNum;

			break;
		}
	}
    if(flag_FreeSpaceFound==false){

    	/////////////////////////////////////////
    	// cout<<"NewPage!!!! @ PageNum= "<<pageNum<<"  // aboveTryPageNum= "<<tryPageNum;
    	//////////////////////////////////////////
    	tryPageNum = pageNum;
    	fileHandle.appendPage(emptyPage);
    	N_slotInPage = 0;
    	F_freeByteInPage = PAGE_SIZE - 4;
    	offsetRecord = 0;
    	offsetSlotDirect = PAGE_SIZE - 4 - 4;


    	tmpNewN = N_slotInPage+1;
    	tmpNewF = F_freeByteInPage-lenRcrdStd-4;

    	rid.slotNum = 0;
    }


    /////////////////////////////////////////
	//cout<<"before Insertion:   lenRcrdStd= "<<lenRcrdStd <<"  N= "<<N_slotInPage<<"  F= "<<F_freeByteInPage<<"  offsetRecord= "<<offsetRecord<<"  offsetSlotDirect= "<<offsetSlotDirect<<endl;
	//if(F_freeByteInPage>PAGE_SIZE) fgetc(stdin);
	//cout<<"offsetRecord="<<offsetRecord<<" offsetSlotDirect="<<offsetSlotDirect<<" rid.slotNum="<<rid.slotNum<<endl;

	//////////////////////////////////////

	rid.pageNum = tryPageNum;
	//cout<<"insert in pageNum "<<rid.pageNum<<" slotNum "<<rid.slotNum<<endl;

	AddRecordInNewpage(newPage, offsetRecord, recordDescriptor, data);
	UpdateDirectInNewPage(newPage, offsetRecord, offsetSlotDirect, lenRcrdStd, tmpNewN, tmpNewF);
	fileHandle.writePage(tryPageNum, newPage);

	/////////////////////////////////////////
	//printRecord(recordDescriptor, data);
	/////////////////////////////////////////



	free(newPage);
	free(emptyPage);

	return 0;
}


RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	bool allowMultiHop = true;
	return innerReadRecord(fileHandle,recordDescriptor, rid, data, allowMultiHop);
}


// data returned from readRecord is "inserted format": [nullIndicator1, realData1] [nullIndicator2, realData2] ...
// not stored format: [ fieldCnt(2Byte) ] [ RcrdOffset 2Byte, 2Byte... ] [realData1] [realData2]
RC RecordBasedFileManager::innerReadRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data, bool allowMultiHop) {

	void* currentPage = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, currentPage);
	int recordBegin = 0;
	int recordLength = 0;
	char* tmpPt = (char*)currentPage + PAGE_SIZE - (2 + 2 + 4*(rid.slotNum+1)) ;

	memcpy(&recordBegin, tmpPt, 2);
	if(recordBegin>>15==1) {
		//cout<<"tombStone in readRecord() "<<endl;
		free(currentPage);
		return -1; 				// tombStone found, record has been deleted
	}


	tmpPt += 2;
	memcpy(&recordLength, tmpPt, 2);
	tmpPt = (char*)currentPage + recordBegin;

	char* recordStart = tmpPt; // the start of record (include info and field data)
	int cntAttri = 0;
	memcpy(&cntAttri, tmpPt, 2);


	if((cntAttri>>15)!=1){
		tmpPt += 2;
		char* dataHead = new char(0);

		void* tmpDataHeadPt = data;
		void* tmpDataFieldPt = (char*)data + (int)ceil((double)cntAttri/8);

		bool nullFlag = false;
		int fieldOffset = 0;
		int tmpContainer = 0;
		int lastOffset = 2 + 2*cntAttri;
		int lengthReturnedField = 0;
		void* tmpDataFieldRaw = malloc(PAGE_SIZE);
		memset(tmpDataFieldRaw, 0, PAGE_SIZE);

		for(int i=0; i<cntAttri; i++){
			memcpy(&tmpContainer, tmpPt, 2);
			tmpPt += 2;

			nullFlag = (tmpContainer >> 15) ;
			fieldOffset = tmpContainer - (nullFlag << 15);

			*dataHead = (*dataHead) | (nullFlag<<(7-i%8)) ;
			if((i+1)%8==0 || i==cntAttri-1){

				///////////////////////////////////////////////////
				//cout<<i<<" ";
				//cout<<nullFlag<<" "<<bitset<2*8>(tmpContainer)<<" "<<bitset<sizeof(char)*8>(*dataHead)<<endl;
				///////////////////////////////////////////////////

				memcpy(tmpDataHeadPt, dataHead, 1);
				memset(dataHead, 0, 1);
				tmpDataHeadPt = (char*)tmpDataHeadPt + 1;  // head part of returned Data

			}



			memcpy( tmpDataFieldRaw, recordStart + lastOffset, fieldOffset - lastOffset); // rawData cut from storage, need one more cut to ReturnedData if VarChar


			lengthReturnedField = fieldOffset - lastOffset;


			if(nullFlag==0){
				memcpy(tmpDataFieldPt, tmpDataFieldRaw, lengthReturnedField);

				////////////////////////////////////////////////////////
				//cout<<lastOffset<<" ~ "<<fieldOffset<<" :";
				//if(recordDescriptor[i].type == TypeReal) cout<<*(float*)tmpDataFieldPt<<endl;
				//else if(recordDescriptor[i].type == TypeInt) cout<<*(int*)tmpDataFieldPt<<endl;
				////////////////////////////////////////////////////////


				tmpDataFieldPt = (char*)tmpDataFieldPt + lengthReturnedField;


			}
			lastOffset = fieldOffset;
		}

		//////////////////
		//cout<<"Read: ";
		//printRecord(recordDescriptor, data);
		//////////////////

		free(currentPage);
		free(tmpDataFieldRaw);
		free(dataHead);

	    return 0;

	}
	// if here is a pointer
	else{
		if(allowMultiHop == true){
			int newPageNum = 0;
			memcpy(&newPageNum, tmpPt+2, 4);
			int newSlotNum = 0;
			memcpy(&newSlotNum , tmpPt+6, 2);
			RID tmpNewRid;
			tmpNewRid.pageNum = newPageNum;
			tmpNewRid.slotNum = newSlotNum;

			free(currentPage);

			return readRecord(fileHandle, recordDescriptor, tmpNewRid, data);
		}
		else{
			free(currentPage);
			//cout<<"MultiHop!!"<<endl;
			return -1;
		}

	}



}



RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	int cntAttribute = recordDescriptor.size();
	int nullFieldsByteCnt =  ceil((double)cntAttribute / 8) ;
	char* tmpNullByte = new char(0);
	int sizeOfField_storage = 0;
	int* lenVarChar = new int(0);
	void* dataPt = (char*)data + nullFieldsByteCnt;
	void* tmpValue = malloc(PAGE_SIZE);
	for(int i=0; i<cntAttribute; i++){
		memcpy(tmpNullByte, (char*)data + i/8, 1);
		// non-null field
		if( ( (*tmpNullByte>>(7-i%8)) & 1 ) == 0 ){
			if(recordDescriptor[i].type == TypeVarChar){
				memcpy(lenVarChar, dataPt, sizeof(int));
				sizeOfField_storage = *lenVarChar;
				dataPt = (char*)dataPt + sizeof(int);
			}
			else{
				sizeOfField_storage = recordDescriptor[i].length;
			}

			memset(tmpValue, 0, PAGE_SIZE);
			memcpy(tmpValue, dataPt, sizeOfField_storage);
			dataPt = (char*)dataPt + sizeOfField_storage;

			switch(recordDescriptor[i].type){
			case TypeInt:
				printf("%s: %d  ", (recordDescriptor[i].name).c_str(), *((int*)tmpValue));
				break;
			case TypeReal:
				printf("%s: %f  ", (recordDescriptor[i].name).c_str(), *((float*)tmpValue));
				break;
			case TypeVarChar:
				printf("%s: %s  ", (recordDescriptor[i].name).c_str(), ((char*)tmpValue));
				break;
			default:
				break;
			}
		}
		// null field
		else{
			printf("%s: NULL  ", (recordDescriptor[i].name).c_str());
		}
	}
	printf("\n");

	free(tmpValue);
	free(lenVarChar);
    return 0;
}



// to move cntOfRcdNeedMove records ahead(if movLen>0) or behind( movLen<0), from ptToMove to ptToMove-moveLen Bytes. And update their slot.Offset. No change For  N, F.
RC RecordBasedFileManager::moveOtherRecordAhead(char* currentPage, char* ptToMove, int moveLen, char* recordOffsetPt, int cntOfRcdNeedMove){

	int offsetDelete = 0;
	memcpy(&offsetDelete, recordOffsetPt, 2);

	char* Fpt = currentPage + PAGE_SIZE - 2;
	char* Npt = currentPage + PAGE_SIZE - 4;
	int F_freeSpace = 0;
	int N_slotCount = 0;
	memcpy(&F_freeSpace, Fpt, 2);
	memcpy(&N_slotCount, Npt, 2);

	int tmpOffset = 0;
	int tmpLen = 0;
	int moveSizeTotal = 0;
	char* tmpPt = currentPage + PAGE_SIZE - 4 - 4; // the first slot

	for(int i=0; i<N_slotCount; i++){
		// Update recordOffset in slot
		memcpy(&tmpOffset, tmpPt, 2);

		// tombStone , jump over
		if(tmpOffset>>15==1){
			tmpPt -= 4;
		}
		else{
			if(tmpOffset > offsetDelete){
				tmpOffset -= moveLen;
				memcpy(tmpPt, &tmpOffset, 2);
				memcpy(&tmpLen, tmpPt+2, 2);
				// for next record (previous slot)
				moveSizeTotal += tmpLen;
				tmpPt -= 4;
			}
			else{
				tmpPt -= 4;
			}
		}
	}

	memmove(ptToMove-moveLen, ptToMove, moveSizeTotal);
	if(moveLen > 0) memset(ptToMove+moveSizeTotal-moveLen, 0, moveLen);

	return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){


	void* currentPage = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, currentPage);
	int recordBegin = 0;
	int recordLength = 0;
	char* tmpPt;
	int N_slotInPage = 0;
	int F_freeByteInPage = 0;

	tmpPt = (char*)currentPage + PAGE_SIZE - 4;
	char* Npt = tmpPt;
	memcpy(&N_slotInPage, tmpPt, 2);

	tmpPt = tmpPt + 2;
	char* Fpt = tmpPt;
	memcpy(&F_freeByteInPage, tmpPt, 2);

	tmpPt = (char*)currentPage + PAGE_SIZE - (2 + 2 + 4*(rid.slotNum +  1)) ;
	char* recordOffsetPt = tmpPt;	// point to the slot pos (where stored the record offset in page, followed record length)

	memcpy(&recordBegin, tmpPt, 2);


	int pageNum = fileHandle.getNumberOfPages();
	if(rid.pageNum >= pageNum) {
		free(currentPage);
		return 0;
	}

	if(rid.slotNum >= N_slotInPage){
		free(currentPage);
		return 0;
	}
	if(recordBegin>>15 == 1){
		free(currentPage);
		return 0; 	// TombStone means already deleted
	}
	else
	{
		tmpPt += 2;
		char* recordLenPt = tmpPt;	// point to the slot pos (stored record length)
		memcpy(&recordLength, tmpPt, 2);

		tmpPt = (char*)currentPage + recordBegin;
		char* cntAttriPt = tmpPt; 	// the start of record (include info and field data)
		int cntAttri = 0;
		memcpy(&cntAttri, tmpPt, 2);

		// whether here is real data or pointer
		// real data:
		if(cntAttri>>15 != 1){
			int cntOfRcdNeedMove = N_slotInPage - (rid.slotNum + 1);
			char* ptToMove = cntAttriPt + recordLength;

			moveOtherRecordAhead((char*)currentPage, ptToMove, recordLength, recordOffsetPt, cntOfRcdNeedMove);

			int tmpNum = 1<<15;
			memcpy(recordOffsetPt, &tmpNum, 2); // build tombstone

			int newF = F_freeByteInPage+recordLength;
			memcpy(Fpt, &newF, 2);

			fileHandle.writePage(rid.pageNum, currentPage);

			free(currentPage);
			return 0;
		}
		// pointer:
		else{

			int newPageNum = 0;
			memcpy(&newPageNum, tmpPt+2, 4);
			int newSlotNum = 0;
			memcpy(&newSlotNum , tmpPt+6, 2);
			RID tmpNewRid;
			tmpNewRid.pageNum = newPageNum;
			tmpNewRid.slotNum = newSlotNum;

			//cout<<"DeletePnt -> newPageNum "<<newPageNum<<" newSlotNum "<<newSlotNum<<endl;

			free(currentPage);

			return deleteRecord(fileHandle, recordDescriptor,tmpNewRid);

		}

	}
}


RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){
	void* currentPage = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, currentPage);
	int recordBegin = 0; // record offset in page ( this variable should stored in (Slot.offset, Slot.length) ) // also indicate if tombstone
	int recordLength = 0; // ...
	char* tmpPt = new char;
	int N_slotInPage = 0;
	int F_freeByteInPage = 0;

	tmpPt = (char*)currentPage + PAGE_SIZE - 4;
	char* Npt = tmpPt;
	memcpy(&N_slotInPage, tmpPt, 2);

	tmpPt = tmpPt + 2;
	char* Fpt = tmpPt;
	memcpy(&F_freeByteInPage, tmpPt, 2);

	tmpPt = (char*)currentPage + PAGE_SIZE - (2 + 2 + 4*(rid.slotNum +  1)) ;
	char* recordOffsetPt = tmpPt;	// point to the slot pos (where stored the record offset in page, followed by record length)

	memcpy(&recordBegin, tmpPt, 2);
	if(recordBegin>>15 == 1){
		free(currentPage);
		return -1; 	// TombStone means already deleted
	}
	else
	{
		tmpPt += 2;
		char* recordLenPt = tmpPt;	// point to the slot pos (stored record length)
		memcpy(&recordLength, tmpPt, 2);

		tmpPt = (char*)currentPage + recordBegin;
		char* cntAttriPt = tmpPt; 	// the start of record (include info and field data)
		int cntAttri = 0;
		memcpy(&cntAttri, tmpPt, 2); // cntAttri also indicate if RealData or Pointer.

		int newFreespace = 0;

		int lenOfNewRecord = lengthOfRecordStored(recordDescriptor, data);

		// if there is a real data
		if(cntAttri>>15 != 1){
			// new record is shorter or equal length
			if(lenOfNewRecord <= recordLength){

				//cout<<"page "<<rid.pageNum<<" slot "<<rid.slotNum<<" (shorter or equal length)"<<endl;

				AddRecordInNewpage(currentPage, recordBegin, recordDescriptor, data);
				moveOtherRecordAhead((char*)currentPage, cntAttriPt + recordLength, recordLength - lenOfNewRecord, recordOffsetPt, N_slotInPage - rid.slotNum - 1);

				memcpy(recordLenPt, &lenOfNewRecord, 2);

				newFreespace = F_freeByteInPage + (recordLength - lenOfNewRecord);
				memcpy(Fpt, &newFreespace, 2);

			}
			// new record is longer, need to  find a new position in new page to store, or move others behind.
			// if need a new page: set the pointer and move others ahead. update offset, N, F.
			else{

				// record position changes to another page, set a pointer (8Bytes) in current page
				if(F_freeByteInPage < lenOfNewRecord){

					int newPageNum, newOffset, NCnt_beforeInsertion;
					RID tmpRid;


					//cout<<"page "<<rid.pageNum<<" slot "<<rid.slotNum<<" --> ";
					// insert @(newPageNum, newOffset), change the newN newF
					insertRecord(fileHandle, recordDescriptor, data, tmpRid);


					//pointer-record in current page
					char* pointerRcrd = new char[8]; // 8Bytes, first 2Bytes are [(1<<15) (pointer indicator)], followed by 4Bytes pagenum(int) , followed 2Bytes are offsetOfRecord
					int tmpIdcIsPt = 1<<15;
					int tmpIdcPageNum = tmpRid.pageNum;
					int tmpIdcSlotNum = tmpRid.slotNum;
					memcpy(pointerRcrd, &tmpIdcIsPt, 2);
					memcpy(pointerRcrd+2, &tmpIdcPageNum, 4);
					memcpy(pointerRcrd+6, &tmpIdcSlotNum, 2);
					memcpy(cntAttriPt, pointerRcrd, 8);
					free(pointerRcrd);

					//update slot.info (length) in current page
					int lenPntRcrd = 8;
					memcpy(recordLenPt, &lenPntRcrd, 2);

					//move other records ahead
					moveOtherRecordAhead((char*)currentPage, cntAttriPt + recordLength, recordLength - 8, recordOffsetPt, N_slotInPage - rid.slotNum - 1);


					//update F in current page
					newFreespace = F_freeByteInPage + recordLength - 8;
					memcpy(Fpt, &newFreespace, 2);
				}
				// record position still in current page
				else{

					//cout<<"page "<<rid.pageNum<<" slot "<<rid.slotNum<<" (longer but stay in page)"<<endl;

					// because now moveLen = recordLength - lenOfNewRecord < 0 , so in fact it's moving behind
					moveOtherRecordAhead((char*)currentPage, cntAttriPt + recordLength, recordLength - lenOfNewRecord, recordOffsetPt, N_slotInPage - rid.slotNum - 1);
					AddRecordInNewpage(currentPage, recordBegin, recordDescriptor, data);
					memcpy(recordLenPt, &lenOfNewRecord, 2);

					newFreespace = F_freeByteInPage + (recordLength - lenOfNewRecord);
					memcpy(Fpt, &newFreespace, 2);
				}

			}
			fileHandle.writePage(rid.pageNum, currentPage);
			free(currentPage);
			return 0;
		}
		// if it is a pointer
		else{

			int newPageNum = 0;
			memcpy(&newPageNum, cntAttriPt+2, 4);
			int newSlotNum = 0;
			memcpy(&newSlotNum , cntAttriPt+6, 2);
			RID tmpNewRid;
			tmpNewRid.pageNum = newPageNum;
			tmpNewRid.slotNum = newSlotNum;

			//cout<<"pointer to page "<<rid.pageNum<<" slot "<<rid.slotNum<<"-->";
			free(currentPage);
			return updateRecord(fileHandle, recordDescriptor, data, tmpNewRid);
		}

	}

}

// returned single-Selected-Attri-data with nullIndicator header
// resAttriDataLength is nude data length, not include NullHeaderLength
RC RecordBasedFileManager::selectAttributeFromRecord(const vector<Attribute> &recordDescriptor, void* data,  const string &attributeName, void *resData, int& resAttriDataLength, AttrType& resType){
	int cntAttribute = recordDescriptor.size();
	int nullFieldsByteCnt =  ceil((double)cntAttribute / 8) ;
	char* tmpNullByte = new char(0);
	int sizeOfField_storage = 0;
	int* lenVarChar = new int(0);
	void* dataPt = (char*)data + nullFieldsByteCnt;
	void* tmpValue = malloc(PAGE_SIZE);
	int tmpNullIndicator = 0;
	for(int i=0; i<cntAttribute; i++){
		memcpy(tmpNullByte, (char*)data + i/8, 1);
		// non-null field
		if( ( (*tmpNullByte>>(7-i%8)) & 1 ) == 0 ){
			if(recordDescriptor[i].type == TypeVarChar){
				memcpy(lenVarChar, dataPt, sizeof(int));
				sizeOfField_storage = *lenVarChar;

				dataPt = (char*)dataPt + sizeof(int);

				if( recordDescriptor[i].name == attributeName )	{

					/////////////////////////////////
					//cout<<i<<" "<<recordDescriptor[i].name<<" "<<attributeName<<" in select... : "<<sizeOfField_storage<<endl;
					////////////////////////////////

					tmpNullIndicator = 0;
					memcpy((char*)resData, &tmpNullIndicator, 1);
					memcpy((char*)resData+1, lenVarChar, sizeof(int));
					memcpy((char*)resData+1+sizeof(int), dataPt, *lenVarChar);
					resAttriDataLength = *lenVarChar;
					resType = TypeVarChar;


					/*
					//////////////////////////////////////////////
					char* tmpStr = new char[100];
					memset(tmpStr, 0 ,100);
					memcpy(tmpStr, (char*)resData+1+sizeof(int), *lenVarChar);
					cout<< tmpStr <<endl;
					//////////////////////////////////////////////

					 */

					memset(tmpValue, 0, PAGE_SIZE);
					memcpy(tmpValue, dataPt, sizeOfField_storage);
					dataPt = (char*)dataPt + sizeOfField_storage;


					break;
				}
				dataPt = (char*)dataPt + *lenVarChar;
			}
			else{
				sizeOfField_storage = recordDescriptor[i].length;

				if( recordDescriptor[i].name == attributeName )	{


					/////////////////////////////////
					//cout<<i<<" "<<recordDescriptor[i].name<<" "<<attributeName<<" in select... : "<<sizeOfField_storage<<endl;
					////////////////////////////////

					tmpNullIndicator = 0;
					memcpy((char*)resData, &tmpNullIndicator, 1);
					memcpy((char*)resData+1, dataPt, sizeOfField_storage);
					resAttriDataLength = sizeOfField_storage;
					resType = recordDescriptor[i].type;


					//////////////////////////////////////////////
					/*
					if(resType==TypeInt){

						cout<< *(int*)((char*)resData+1)<<endl;

					}
					else{
						cout<< *(float*)((char*)resData+1)<<endl;
					}
					*/
					//////////////////////////////////////////////

					memset(tmpValue, 0, PAGE_SIZE);
					memcpy(tmpValue, dataPt, sizeOfField_storage);
					dataPt = (char*)dataPt + sizeOfField_storage;

					break;
				}
				dataPt = (char*)dataPt + sizeOfField_storage;
			}

		}
		// null field
		else{
			if( recordDescriptor[i].name == attributeName )	{


				/////////////////////////////////
				//cout<<i<<" in select... :NULL "<<sizeOfField_storage<<endl;
				////////////////////////////////

				tmpNullIndicator = 1<<7;
				memcpy(resData, &tmpNullIndicator, 1);
				resAttriDataLength = 0;
				resType = recordDescriptor[i].type;
				break;
			}
		}
	}


	///////////////////////////////////////////////////////////
	//printRecord(recordDescriptor, resData);
	///////////////////////////////////////////////////////////
	delete(tmpNullByte);
	delete(lenVarChar);
	free(tmpValue);
    return 0;

}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
	void* formatedRcrd = malloc(2000);
	int resAttriDataLength;
	AttrType resType;
	int readNotTombsotne = readRecord(fileHandle, recordDescriptor, rid, formatedRcrd);
	if(readNotTombsotne == -1) return -1;

	selectAttributeFromRecord(recordDescriptor, formatedRcrd, attributeName, data, resAttriDataLength, resType);
	free(formatedRcrd);
	return 0;
}
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
    const vector<Attribute> &recordDescriptor,
    const string &conditionAttribute,
    const CompOp compOp,                  // comparision type such as "<" and "="
    const void *value,                    // used in the comparison
    const vector<string> &attributeNames, // a list of projected attributes
    RBFM_ScanIterator &rbfm_ScanIterator,
	RecordBasedFileManager *rbfm)
{

	rbfm_ScanIterator.initiate(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames,rbfm);

	return 0;
}



RBFM_ScanIterator:: RBFM_ScanIterator(){
}
RBFM_ScanIterator:: ~RBFM_ScanIterator(){
}

RC RBFM_ScanIterator:: initiate(FileHandle &fileHandle,
    const vector<Attribute> &recordDescriptor,
    const string &conditionAttribute,
    const CompOp compOp,                  // comparision type such as "<" and "="
    const void *value,                    // used in the comparison
    const vector<string> &attributeNames,
	RecordBasedFileManager *rbfm){

	this->fileHandle = fileHandle;
	this->filePageNum = fileHandle.getNumberOfPages();
	(this->recordDescriptor).assign(recordDescriptor.begin(), recordDescriptor.end());
	this->conditionAttribute = conditionAttribute;
	this->compOp = compOp;
	this->value = value;


	//cout<<"inInitiate value       "<< (char*)value + sizeof(int)<<endl;
	//cout<<"inInitiate this->value "<<  (char*)(this->value) + sizeof(int) <<endl;


	//for(int i=0; i<attributeNames.size(); i++){
	//	//cout<<"### AttriCnt "<<i<<endl;
	//	this->attributeNames.push_back(attributeNames[i]);
	//}

	this->attributeNames.assign(attributeNames.begin(), attributeNames.end());

	this->ridSet = false;
	this->currentPage = malloc(PAGE_SIZE);
	this->rcrdCnt = 0;
	this->rbfm = rbfm;

	this->rid.pageNum = 0;
	this->rid.slotNum = 0;
	return 0;
}

  // Never keep the results in the memory. When getNextRecord() is called,
  // a satisfying record needs to be fetched from the file.
  // "data" follows the same format as RecordBasedFileManager::insertRecord().
RC RBFM_ScanIterator:: getNextRecord(RID &rid, void *data){

	/*
	if(this->ridSet == false){
		this->rid.pageNum = rid.pageNum;
		this->rid.slotNum = rid.slotNum;
		this->ridSet = true;
	}
	*/

	char* resAttriData = (char*)malloc(PAGE_SIZE);
	char* resData = (char*)malloc(PAGE_SIZE);
	void* tmpRcrdVal = malloc(PAGE_SIZE);
	char* readRcrdData = (char*)malloc(PAGE_SIZE);

	while(1){

		memset(currentPage, 0, PAGE_SIZE);

		memset(readRcrdData, 0, PAGE_SIZE);
		memset(resAttriData, 0, PAGE_SIZE);
		memset(resData, 0, PAGE_SIZE);
		memset(tmpRcrdVal, 0, PAGE_SIZE);


		if(this->rid.pageNum>=filePageNum){
			//cout<<"!!!!rid.pageNum>=filePageNum"<<endl;

			free(readRcrdData);
			free(resAttriData);
			free(resData);
			free(tmpRcrdVal);

			return RBFM_EOF;
		}


		fileHandle.readPage(this->rid.pageNum, currentPage);

		int N = 0;
		memcpy(&N, (char*)currentPage+PAGE_SIZE-4, 2);


		/*
		///////////////////////////////////////////////

		cout<<"+++++++++++++++++++++++++++"<<endl;
		cout<<"N "<<N<<endl;
		cout<<"this->rid.pageNum "<<this->rid.pageNum<<endl;
		cout<<"this->rid.slotNum "<<this->rid.slotNum<<endl;
		cout<<"filePageNum "<<filePageNum<<endl;
		cout<<"rcrdCnt "<<rcrdCnt<<endl;
		cout<<"+++++++++++++++++++++++++++"<<endl;
		//////////////////////////////////////////////
		*/

		if(rcrdCnt>=N){
			rcrdCnt = 0;
			//rid.pageNum = this->rid.pageNum;  /////////???????????? what rid should return if not found qualified rcrd? end of file? last one recrd?
			//rid.slotNum = this->rid.slotNum;
			this->rid.pageNum++;
			this->rid.slotNum=0;
			continue;
		}
		else{
			bool allowMultiHop = false;
			int readNotTombstone = rbfm->innerReadRecord(this->fileHandle, this->recordDescriptor, this->rid, readRcrdData, allowMultiHop);

			///////////////////////////////////////////////////////////
			//cout<<"~~~~ in getNextRecord: ";
			//rbfm->printRecord(this->recordDescriptor, readRcrdData);
			///////////////////////////////////////////////////////////

			if(readNotTombstone == -1){
				rcrdCnt ++;
				//cout<<"a tombStone "<<endl;
				this->rid.slotNum ++;
				continue;
			}
			else{
				rcrdCnt ++;
				this->rid.slotNum ++;

				int resAttriDataLength;
				AttrType resType;
				bool qualifiedRcrd = true;
				int nullIndicatorLen = ceil(double(attributeNames.size())/8.0);
				char* headerPt = resData;
				char* dataPt = resData+nullIndicatorLen;
				int tmpNullIndicator;
				int allNullIndicator=0;
				int attriAddedCnt = 0;

				memset(resData, 0, PAGE_SIZE);

				int tmpReturnedDataLen=nullIndicatorLen;

				for(int i=0; i<(int)recordDescriptor.size(); i++){
					rbfm->selectAttributeFromRecord(recordDescriptor, readRcrdData, recordDescriptor[i].name, resAttriData, resAttriDataLength, resType);


					/*
					////////////////////////////////////////////////////////////////////////
					//cout<<"attributeNames size = "<<attributeNames.size()<<endl;
					//cout<<"from Select Func "<<i<<" "<<recordDescriptor[i].name<<" "<<resAttriDataLength<<" "<<resType<<endl;
					cout<<endl;
					cout<<"record No. "<<rcrdCnt<<"  Field No. "<<i<<endl;
					cout<<"CntAttriinTarget : "<<" "<<count(attributeNames.begin(), attributeNames.end(), recordDescriptor[i].name)<<" ";
					cout<<"rcrdDscrpt(CurrentVisit): "<<" "<<recordDescriptor[i].name<<endl;
					//for(int j=0; j<attributeNames.size(); j++) cout<<"size "<<attributeNames.size()<<"  attributeName(Target): "<<attributeNames[j]<<endl;
					////////////////////////////////////////////////////////////////////////
					*/

					tmpNullIndicator = 0;
					memcpy(&tmpNullIndicator, resAttriData, 1);

					if(count(attributeNames.begin(), attributeNames.end(), recordDescriptor[i].name)>0 ){
						allNullIndicator = allNullIndicator | ((tmpNullIndicator>>7)<<(7-attriAddedCnt%8));

						memset(tmpRcrdVal, 0, PAGE_SIZE);
						if((attriAddedCnt+1)%8==0 ) {
							memcpy(headerPt, &allNullIndicator, 1);
							headerPt++;
							allNullIndicator = 0;
						}

						if(tmpNullIndicator>>7 != 1){
							if(resType== TypeVarChar){
								memcpy(dataPt, resAttriData+1, sizeof(int)+resAttriDataLength);
								memcpy(tmpRcrdVal, resAttriData+1+sizeof(int), resAttriDataLength);
								dataPt += sizeof(int)+resAttriDataLength;

								tmpReturnedDataLen += sizeof(int)+resAttriDataLength;

								//cout<<"delta TypeVarChar "<<sizeof(int)+resAttriDataLength<<endl;
								//char* tmpxx = new char[100];
								//memset(tmpxx, 0, 100);
								//memcpy(tmpxx, dataPt-resAttriDataLength, resAttriDataLength);
								//cout<<"Need To Return:(from Pt) "<<recordDescriptor[i].name<<" varchar  "<< tmpxx <<endl;

							}
							else{
								memcpy(dataPt, resAttriData+1, resAttriDataLength);
								memcpy(tmpRcrdVal, resAttriData+1, resAttriDataLength);
								dataPt += resAttriDataLength;
								int* tbId = new int(0);
								memcpy(tbId, dataPt-resAttriDataLength, 4);

								tmpReturnedDataLen += resAttriDataLength;

								/*
								////////////////////////
								if(resType==TypeInt){

									char* tmpxx = new char[100];
									memcpy(tmpxx, dataPt-resAttriDataLength, resAttriDataLength);

									cout<<"Need To Return:(from Pt) "<<recordDescriptor[i].name<<" int  "<< *(int*)tmpxx <<endl;

								}
								else{
									char* tmpxx = new char[100];
									memcpy(tmpxx, dataPt-resAttriDataLength, resAttriDataLength);

									cout<<"Need To Return:(from Pt) "<<recordDescriptor[i].name<<" int  "<< *(float*)tmpxx <<endl;

								}
								////////////////////////
								*/
							}
						}

						attriAddedCnt++;
					}
					else{

						if(tmpNullIndicator>>7 != 1){
							memset(tmpRcrdVal, 0, PAGE_SIZE);

							if(resType== TypeVarChar){
								memcpy(tmpRcrdVal, resAttriData+1+sizeof(int), resAttriDataLength);
							}
							else{
								memcpy(tmpRcrdVal, resAttriData+1, resAttriDataLength);
							}
						}
					}


					if(attriAddedCnt==(int)(attributeNames.size())-1) {
						memcpy(headerPt, &allNullIndicator, 1);
						headerPt++;
						allNullIndicator = 0;
					}


					/*
					////////////////////
					cout<<"from tmpRcrdVal: ";
					if(resType== TypeVarChar){
						string tmpx = (char*)tmpRcrdVal;
						cout<<recordDescriptor[i].name<<" varchar  "<< tmpx <<endl;
					}
					else if(resType== TypeInt){
						cout<<recordDescriptor[i].name<<" int  "<< *(int*)tmpRcrdVal<<endl;
					}
					else{
						cout<<recordDescriptor[i].name<<"  float  "<< *(float*)tmpRcrdVal<<endl;
					}
					///////////////////

					*/


					if(compOp==NO_OP){
						qualifiedRcrd = true;
						continue;
					}
					if((this->value == NULL) || (tmpNullIndicator>>7 == 1)){
						qualifiedRcrd = false;
					}
					else{
						if(this->conditionAttribute == recordDescriptor[i].name ){
							if(resType==TypeVarChar){
								string tmpRcrdValStr = (char*)tmpRcrdVal;
								string conditionVal = (char*)(this->value)+sizeof(int);

								switch(this->compOp){
								case EQ_OP: qualifiedRcrd = (tmpRcrdValStr==conditionVal); /* cout<<"/// "<<i<<" "<<tmpRcrdValStr<<"=="<<conditionVal<<" ? "<<qualifiedRcrd<<endl; */ break; // no condition// =
								case LT_OP: qualifiedRcrd = (tmpRcrdValStr<conditionVal); /* cout<<"/// "<<i<<" "<<tmpRcrdValStr<<"<"<<conditionVal<<" ? "<<qualifiedRcrd<<endl; */ break;     // <
								case LE_OP: qualifiedRcrd = (tmpRcrdValStr<=conditionVal); /* cout<<"/// "<<i<<" "<<tmpRcrdValStr<<"<="<<conditionVal<<" ? "<<qualifiedRcrd<<endl; */ break;     // <=
								case GT_OP: qualifiedRcrd = (tmpRcrdValStr>conditionVal); /* cout<<"/// "<<i<<" "<<tmpRcrdValStr<<">"<<conditionVal<<" ? "<<qualifiedRcrd<<endl; */ break;     // >
								case GE_OP: qualifiedRcrd = (tmpRcrdValStr>=conditionVal); /* cout<<"/// "<<i<<" "<<tmpRcrdValStr<<">="<<conditionVal<<" ? "<<qualifiedRcrd<<endl; */ break;     // >=
								case NE_OP: qualifiedRcrd = (tmpRcrdValStr!=conditionVal); /* cout<<"/// "<<i<<" "<<tmpRcrdValStr<<"!="<<conditionVal<<" ? "<<qualifiedRcrd<<endl; */ break;     // !=
								case NO_OP: qualifiedRcrd = true; break;
								default: qualifiedRcrd = true; break;
								}
							}
							else if(resType==TypeInt){
								int tmpRcrdValStr = *(int*)tmpRcrdVal;
								int conditionVal = *(int*)(this->value);

								switch(this->compOp){
								case EQ_OP: qualifiedRcrd = (tmpRcrdValStr==conditionVal); /* cout<<"/// "<<i<<" "<<tmpRcrdValStr<<"=="<<conditionVal<<" ? "<<qualifiedRcrd<<endl; */  break; // no condition// =
								case LT_OP: qualifiedRcrd = (tmpRcrdValStr<conditionVal); /* cout<<"/// "<<i<<" "<<tmpRcrdValStr<<"<"<<conditionVal<<" ? "<<qualifiedRcrd<<endl; */ break;     // <
								case LE_OP: qualifiedRcrd = (tmpRcrdValStr<=conditionVal); /* cout<<"/// "<<i<<" "<<tmpRcrdValStr<<"<="<<conditionVal<<" ? "<<qualifiedRcrd<<endl; */ break;     // <=
								case GT_OP: qualifiedRcrd = (tmpRcrdValStr>conditionVal); /* cout<<"/// "<<i<<" "<<tmpRcrdValStr<<">"<<conditionVal<<" ? "<<qualifiedRcrd<<endl; */ break;     // >
								case GE_OP: qualifiedRcrd = (tmpRcrdValStr>=conditionVal); /* cout<<"/// "<<i<<" "<<tmpRcrdValStr<<">="<<conditionVal<<" ? "<<qualifiedRcrd<<endl; */ break;     // >=
								case NE_OP: qualifiedRcrd = (tmpRcrdValStr!=conditionVal); /* cout<<"/// "<<i<<" "<<tmpRcrdValStr<<"!="<<conditionVal<<" ? "<<qualifiedRcrd<<endl; */ break;     // !=
								case NO_OP: qualifiedRcrd = true; break;
								default: qualifiedRcrd = true; break;
								}
							}
							else if(resType==TypeReal){
								float tmpRcrdValStr = *(float*)tmpRcrdVal;
								float conditionVal = *(float*)(this->value);

								switch(this->compOp){
								case EQ_OP: qualifiedRcrd = (tmpRcrdValStr==conditionVal); /* cout<<"/// "<<i<<" "<<tmpRcrdValStr<<"=="<<conditionVal<<" ? "<<qualifiedRcrd<<endl; */ break; // no condition// =
								case LT_OP: qualifiedRcrd = (tmpRcrdValStr<conditionVal); /* cout<<"/// "<<i<<" "<<tmpRcrdValStr<<"<"<<conditionVal<<" ? "<<qualifiedRcrd<<endl; */ break;     // <
								case LE_OP: qualifiedRcrd = (tmpRcrdValStr<=conditionVal); /* cout<<"/// "<<i<<" "<<tmpRcrdValStr<<"<="<<conditionVal<<" ? "<<qualifiedRcrd<<endl; */ break;     // <=
								case GT_OP: qualifiedRcrd = (tmpRcrdValStr>conditionVal); /* cout<<"/// "<<i<<" "<<tmpRcrdValStr<<">"<<conditionVal<<" ? "<<qualifiedRcrd<<endl; */ break;     // >
								case GE_OP: qualifiedRcrd = (tmpRcrdValStr>=conditionVal); /* cout<<"/// "<<i<<" "<<tmpRcrdValStr<<">="<<conditionVal<<" ? "<<qualifiedRcrd<<endl; */ break;     // >=
								case NE_OP: qualifiedRcrd = (tmpRcrdValStr!=conditionVal); /* cout<<"/// "<<i<<" "<<tmpRcrdValStr<<"!="<<conditionVal<<" ? "<<qualifiedRcrd<<endl; */ break;     // !=
								case NO_OP: qualifiedRcrd = true; break;
								default: qualifiedRcrd = true; break;
								}

							}
						}
						if(qualifiedRcrd==false) {
							break;
						}


					}

				}
				if(qualifiedRcrd==true){
					//cout<<"%%%%%%% Yes got Record No. "<<rcrdCnt<<endl;

					memcpy(data, resData, tmpReturnedDataLen); //
					rid.pageNum = this->rid.pageNum;
					rid.slotNum = this->rid.slotNum-1;

					free(readRcrdData);
					free(resAttriData);
					free(resData);
					free(tmpRcrdVal);
					return 0;
				}
			}

		}


	}
	//free(currentPage);

}

RC RBFM_ScanIterator:: close(){
	rbfm->closeFile(this->fileHandle);
	return 0;
}
