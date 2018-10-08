
#include "qe.h"

// not include null indicator length
int lengthOfInsertFormat(vector<Attribute> &recordDescriptor, void* data){
	int cntAttribute = recordDescriptor.size();
	int lenRcrdStd = 0;
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

				dataPt = (char*)dataPt + sizeOfField_storage;
				lenRcrdStd += sizeOfField_storage;
			}

	}

	free(lenVarChar);
	free(tmpNullByte);
    return lenRcrdStd;
}

RC joinTuple(void* data, void* leftDataPt, void* rightDataPt, vector<Attribute>& leftAttris, vector<Attribute>& rightAttris){

	int lenHeaderL = ceil((double)leftAttris.size()/8);
	int lenHeaderR = ceil((double)rightAttris.size()/8);
	int lenHeaderJoin = ceil((double)( leftAttris.size()+rightAttris.size() ) /8);

	int cnt = 0;
	char tmpVal = 0, bitNull = 0, byteNull = 0;

	for( int i=0; i<leftAttris.size(); i++){
		tmpVal = 0;	bitNull = 0;
		memcpy(&tmpVal, (char*)leftDataPt + i/8, 1);
		bitNull = tmpVal>>(7-i%8);
		byteNull = byteNull | (bitNull << (7-cnt%8));
		if( (cnt+1)%8 == 0){
			memcpy( (char*)data + (cnt/8), &byteNull, 1);
			byteNull = 0;
		}
		cnt++;
	}
	for( int i=0; i<rightAttris.size(); i++){
		tmpVal = 0;	bitNull = 0;
		memcpy(&tmpVal, (char*)rightDataPt + i/8, 1);
		bitNull = tmpVal>>(7-i%8);
		byteNull = byteNull | (bitNull << (7-cnt%8));
		if( (cnt+1)%8 == 0 || i==rightAttris.size()-1 ){
			memcpy( (char*)data + (cnt/8), &byteNull, 1);
			byteNull = 0;
		}
		cnt++;
	}


	int lenDataL = lengthOfInsertFormat(leftAttris, leftDataPt);
	int lenDataR = lengthOfInsertFormat(rightAttris, rightDataPt);

	memcpy( (char*)data + lenHeaderJoin, (char*)leftDataPt + lenHeaderL, lenDataL );
	memcpy( (char*)data + lenHeaderJoin + lenDataL, (char*)rightDataPt + lenHeaderR, lenDataR);
	return 0;
}

// get a nude (for varchar, w/o lenHead) value prcdVal (can be used to Compare()), and a whole value (fro varchar, w/ lenHead) prcdWholeVal
//  of a single required attribute from a complete record data (inserted format)
RC getValFromRcrd(void* prcdVal, void* prcdWholeVal, void* dataIn, vector<Attribute> attrs, string reqAttr, int& numOfAttr){
	int tmpOffset = 0;
	int byteForNullInd = ceil( (double)attrs.size() / 8 );
	void* data = (void*)((char*)dataIn + byteForNullInd);
	for(int i=0; i<attrs.size(); i++){

		int isNullByte = 0;
		memcpy(&isNullByte, (char*)dataIn + i / 8, 1);
		int isNullbit = isNullByte & (1 << ( 7 - i % 8));

		if(attrs[i].name == reqAttr){

				numOfAttr = i;
				if(isNullbit==1){
					prcdVal = NULL;
					prcdWholeVal = NULL;
					return 0;
				}
				else{
					if(attrs[i].type == TypeVarChar){
						int tmpLen = 0;
						memcpy(&tmpLen, data, 4);
						memcpy(prcdVal, (char*)data + tmpOffset + 4, tmpLen);
						memcpy(prcdWholeVal, (char*)data + tmpOffset, 4+tmpLen);
						return 0;
					}
					else{
						memcpy(prcdVal, (char*)data + tmpOffset, 4);
						memcpy(prcdWholeVal, (char*)data + tmpOffset, 4);
						return 0;
					}
				}
		}
		else{

			if(isNullbit==1){
				continue;
			}
			else{
				if(attrs[i].type == TypeVarChar){
					int tmpLen = 0;
					memcpy(&tmpLen, data, 4);
					tmpOffset += 4+tmpLen;
				}
				else{
					tmpOffset += 4;
				}
			}
		}
	}
	return -1;
}

// get a nude value of a required attribute from a inserted format record srcRcrdPt
RC getSelectedAttriVal(void* resAttrPt, void* srcRcrdPt, vector<Attribute>& attris, Attribute targetAttri){
	int resAttriDataLength;
	AttrType resType;
	void* tmpResAttrPt = malloc(PAGE_SIZE);
	memset(tmpResAttrPt, 0, PAGE_SIZE);
	RecordBasedFileManager::selectAttributeFromRecord( attris,  srcRcrdPt, targetAttri.name, tmpResAttrPt, resAttriDataLength, resType);
	int nullByte = 0;
	memcpy(&nullByte, tmpResAttrPt, 1);
	if(nullByte>>7 == 1) resAttrPt = NULL;
	else memcpy(resAttrPt, (char*)tmpResAttrPt + 1, resAttriDataLength);
	free(tmpResAttrPt);
	return 0;
}

///////////////////////// Filter /////////////////////////



bool Filter::judgeQuality(void* valL, CompOp op, void* valR, Attribute attr){
	// -1: key1<key2
	// 0: key1=key2
	// 1: key1>key2
	// 3: key1 is NULL
	// 4: key2 is NULL
	int compRes = IndexManager::compareKey(attr, valL, 0, 0, valR, 0, 0);
	bool qualifiedRcrd = false;
	if(compRes!=3 && compRes!=4){
		switch(op){
		case EQ_OP: qualifiedRcrd = (compRes==0);  break; // ==
		case LT_OP: qualifiedRcrd = (compRes==-1);  break;     // <
		case LE_OP: qualifiedRcrd = (compRes==0 || compRes==-1);  break;     // <=
		case GT_OP: qualifiedRcrd = (compRes==1);  break;     // >
		case GE_OP: qualifiedRcrd = (compRes==0 || compRes==1);  break;     // >=
		case NE_OP: qualifiedRcrd = (compRes!=0);  break;     // !=
		case NO_OP: qualifiedRcrd = true; break;
		default: qualifiedRcrd = false; break;
		}
	}
	return qualifiedRcrd;
}
Filter::Filter(Iterator* input, const Condition &condition) {
	//this->tis = (Filter*)malloc(PAGE_SIZE);  // I really don't know how to initiate such a virtualClassObjectPointer
	this->tis = input;
	// let pt1 = pt2, then not only the value equals but also their address become same!
	// they actually become the same memory piece with 2 names!
	// So  [ DO NOT FREE ], but should [ SET NULL ] in the dissolution func or it would delete the shared piece of memory!
	this->cnd = condition;
	this->attrs.clear();
	input->getAttributes(this->attrs);
}

// ... the rest of your implementations go here

Filter::~Filter(){
	//free(this->tis); // very IMPORTANT !!!!!!!!!
	tis = NULL;
}

RC Filter::getNextTuple(void *data) {
	void* prcdValL = malloc(PAGE_SIZE);
	void* prcdWholeValL = malloc(PAGE_SIZE);
	Attribute selAttr;
	while(this->tis->getNextTuple(data) != QE_EOF){
		memset(prcdValL, 0, PAGE_SIZE);
		memset(prcdWholeValL, 0, PAGE_SIZE);
		int numOfAttr;
		getValFromRcrd(prcdValL, prcdWholeValL, data, this->attrs, this->cnd.lhsAttr, numOfAttr);
		selAttr = (this->attrs)[numOfAttr];
		bool isQualified = judgeQuality(prcdWholeValL, this->cnd.op, (this->cnd).rhsValue.data, selAttr);
		if (isQualified==true){
			free(prcdValL);
			free(prcdWholeValL);
			return 0;
		}
	}
	free(prcdValL);
	free(prcdWholeValL);
	this->tis = NULL;
	return QE_EOF;
}

// For attribute in vector<Attribute>, name it as rel.attr
void Filter::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	for(int i=0; i<this->attrs.size(); i++){
		attrs.push_back(this->attrs[i]);
	}
}



///////////////////////// Block Nested Loop Join /////////////////////////

BNLJoin:: BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numPages       // # of pages that can be loaded into memory,
		                                 	 //   i.e., memory block size (decided by the optimizer)
        ){
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->cnd = condition;
	this->numPage = numPages;

	this->leftIn->getAttributes(this->leftAttris);
	this->rightIn->getAttributes(this->rightAttris);
	for(int i=0; i<this->leftAttris.size(); i++){
		if((this->leftAttris)[i].name == cnd.lhsAttr) this->leftAttriSld = (this->leftAttris)[i];
	}
	for(int i=0; i<this->rightAttris.size(); i++){
		if((this->rightAttris)[i].name == cnd.rhsAttr) this->rightAttriSld = (this->rightAttris)[i];
	}

	this->newRnd = true;
	this->finLeft = false;
	this->finRight = true; // firstly assume right finished so we read leftChunck

	this->leftDataPt = malloc(PAGE_SIZE);
	memset(leftDataPt, 0, PAGE_SIZE);
	this->rightDataPt = malloc(PAGE_SIZE);
	memset(rightDataPt, 0, PAGE_SIZE);
	this->leftDataSldPt = malloc(PAGE_SIZE);
	memset(leftDataSldPt, 0, PAGE_SIZE);
	this->rightDataSldPt = malloc(PAGE_SIZE);
	memset(rightDataSldPt, 0, PAGE_SIZE);

	outerSpace = malloc((this->numPage-2)*PAGE_SIZE );
	spaceUsed = 0;

	this->strMap.clear();
	this->intMap.clear();
	this->fltMap.clear();
}

BNLJoin:: ~BNLJoin(){

	// without "leftIn = new Iterator*..." blah blah... But directly "leftIn = pterIn" :
	// leftIn pterIn: their adresses are same!!! they actually are same piece of memory with two calling names!!!!
	// For leftIn, rightIn:  [ DO NOT FREE ], but should [ SET NULL ] in the dissolution func !!!!!!!
	// or it would delete the shared piece of memory which stilled by outer iterator who sent in!!!!!!!
	this->leftIn = NULL;
	this->rightIn = NULL;

	free(this->leftDataPt);
	free(this->rightDataPt);
	free(this->leftDataSldPt);
	free(this->rightDataSldPt);
	free(outerSpace);
}

RC BNLJoin:: getNextTuple(void *data){
	int tmpDataSize, tmpHeaderSize;
	if(this->finLeft == true && this->finRight == true) return -1;
	if(this->finRight == true){
		int outerOffset = 0;
		this->spaceUsed = 0;
		memset(outerSpace, 0, (this->numPage-2)*PAGE_SIZE );
		while(1){

			memset(this->leftDataPt, 0, PAGE_SIZE);
			memset(this->leftDataSldPt, 0, PAGE_SIZE);
			RC rc = this->leftIn->getNextTuple(this->leftDataPt) ;
			if(rc == -1){
				finLeft = true; break;
			}
			tmpDataSize = lengthOfInsertFormat(this->leftAttris, leftDataPt);
			tmpHeaderSize = ceil( float(this->leftAttris.size()) / 8);
			memcpy( (char*)outerSpace + outerOffset, leftDataPt, tmpDataSize + tmpHeaderSize);
			//outerTupleOffset.push_back(outerOffset);
			//outerTupleLen.push_back(tmpDataSize + tmpHeaderSize);
			getSelectedAttriVal(this->leftDataSldPt, this->leftDataPt, this->leftAttris, this->leftAttriSld);

			if(this->leftAttriSld.type == TypeVarChar ){
				string tmpValx = (char*)(this->leftDataSldPt);
				this->strMap.insert(make_pair( tmpValx, MemOffset(outerOffset, tmpDataSize + tmpHeaderSize) ));
			}
			else if(this->leftAttriSld.type == TypeInt ){
				int tmpValx = *((int*)(this->leftDataSldPt));
				this->intMap.insert(make_pair( tmpValx, MemOffset(outerOffset, tmpDataSize + tmpHeaderSize) ));
			}
			else{
				float tmpValx = *((float*)(this->leftDataSldPt));
				this->fltMap.insert(make_pair( tmpValx, MemOffset(outerOffset, tmpDataSize + tmpHeaderSize) ));
			}

			outerOffset += tmpDataSize + tmpHeaderSize;
			if( outerOffset >= (this->numPage-2.5)*PAGE_SIZE ){
				finLeft = true; break;
			}
		}
		this->finRight = false;
	}

	//leftGetNext
	//judgeIfQualified (hash find )
	//Join
	void* tmpleftVal = malloc(PAGE_SIZE);
	memset(tmpleftVal, 0, PAGE_SIZE);
	memset(this->rightDataPt, 0, PAGE_SIZE);
	memset(this->rightDataSldPt, 0, PAGE_SIZE);
	RC rc = this->rightIn->getNextTuple(this->rightDataPt) ;
	if(rc==-1){
		finRight = true;
		return this->getNextTuple(data);
	}

	bool gotFlag = false;
	getSelectedAttriVal(this->rightDataSldPt, this->rightDataPt, this->rightAttris, this->rightAttriSld);
	if(this->rightAttriSld.type == TypeVarChar){
		string rightVal = (string)( (char*)(this->rightDataSldPt));
		map<string, MemOffset>:: iterator it;
		it = strMap.find(rightVal);
		if(it != strMap.end()){
			memcpy(tmpleftVal, (char*)outerSpace + it->second.offsetM,  it->second.lengthM);
			joinTuple(data, tmpleftVal, this->rightDataPt, this->leftAttris, this->rightAttris);
			gotFlag = true;
		}
	}
	else if(this->rightAttriSld.type == TypeInt){
		int rightVal = *(int*)(this->rightDataSldPt);
		map<int, MemOffset>:: iterator it;
		it = intMap.find(rightVal);
		if(it != intMap.end()){
			memcpy(tmpleftVal, (char*)outerSpace + it->second.offsetM,  it->second.lengthM);
			joinTuple(data, tmpleftVal, this->rightDataPt, this->leftAttris, this->rightAttris);
			gotFlag = true;

		}
	}
	else{
		float rightVal = *(float*)(this->rightDataSldPt);
		map<float, MemOffset>:: iterator it;
		it = fltMap.find(rightVal);
		if(it != fltMap.end()){
			memcpy(tmpleftVal, (char*)outerSpace + it->second.offsetM,  it->second.lengthM);
			joinTuple(data, tmpleftVal, this->rightDataPt, this->leftAttris, this->rightAttris);
			gotFlag = true;

		}
	}

	free(tmpleftVal);
	if(gotFlag == true) return 0;
	else return this->getNextTuple(data);

}
// For attribute in vector<Attribute>, name it as rel.attr
void BNLJoin:: getAttributes(vector<Attribute> &attrs) const{

	attrs.clear();
	for(int i=0; i< this->leftAttris.size(); i++ ) attrs.push_back(this->leftAttris[i]);
	for(int i=0; i< this->rightAttris.size(); i++ ) attrs.push_back(this->rightAttris[i]);
}



///////////////////////// Index Nested Loop Join /////////////////////////

// < <= not done carefully
//leftDataPt, rightDataPt is "inserted()" format: NullHeader + field1 + field2 +...(if varchar: len+realData);
//leftDataSldPt rightDataSLdPt is my stored format: 2Byte field cnt + offset1 + offset2 +... + field1 + field2 (if varchar: len+realData)


INLJoin:: INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        ){
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->cnd = condition;
	this->leftIn->getAttributes(this->leftAttris);
	this->rightIn->getAttributes(this->rightAttris);

	for(int i=0; i<this->leftAttris.size(); i++){
		if((this->leftAttris)[i].name == cnd.lhsAttr) this->leftAttriSld = (this->leftAttris)[i];
	}
	for(int i=0; i<this->rightAttris.size(); i++){
		if((this->rightAttris)[i].name == cnd.rhsAttr) this->rightAttriSld = (this->rightAttris)[i];
	}

	this->newRnd = true;

	this->leftDataPt = malloc(PAGE_SIZE);
	memset(leftDataPt, 0, PAGE_SIZE);
	this->rightDataPt = malloc(PAGE_SIZE);
	memset(rightDataPt, 0, PAGE_SIZE);
	this->leftDataSldPt = malloc(PAGE_SIZE);
	memset(leftDataSldPt, 0, PAGE_SIZE);
	this->rightDataSldPt = malloc(PAGE_SIZE);
	memset(rightDataSldPt, 0, PAGE_SIZE);
}
INLJoin:: ~INLJoin(){
	this->leftIn = NULL;
	this->rightIn = NULL;
	free(this->leftDataPt);
	free(this->rightDataPt);
	free(this->leftDataSldPt);
	free(this->rightDataSldPt);
}



RC INLJoin::getNextTuple(void *data){

	if(this->leftAttriSld.type != this->rightAttriSld.type) return -1;

	if(this->newRnd == true){
		memset(this->leftDataPt, 0, PAGE_SIZE);
		memset(this->leftDataSldPt, 0, PAGE_SIZE);
		if( this->leftIn->getNextTuple(this->leftDataPt) != 0 ){
			return QE_EOF;
		}
		else{
			getSelectedAttriVal(this->leftDataSldPt, this->leftDataPt, this->leftAttris, this->leftAttriSld);

			// if join-focus attribute is this indexFile's attribute and also the cnd.op is (==,<,<=), we can benefit from B+tree;
			// or we have to scan the indexFile from beginning
			if((this->rightAttriSld.name == this->rightIn->attrName) &&
					( (this->cnd.op == EQ_OP) ||  (this->cnd.op == LT_OP) || (this->cnd.op == LE_OP) )){
				this->rightIn->setIterator( leftDataSldPt, NULL, true, true );
			}
			else{
				this->rightIn->setIterator( NULL, NULL, true, true );
			}
			this->newRnd = false;
		}
	}

	memset(this->rightDataPt, 0, PAGE_SIZE);
	memset(this->rightDataSldPt, 0, PAGE_SIZE);
	while(this->rightIn->getNextTuple(this->rightDataPt) != QE_EOF){
		getSelectedAttriVal(this->rightDataSldPt, this->rightDataPt, this->rightAttris, this->rightAttriSld);
		if( Filter::judgeQuality( this->leftDataSldPt, this->cnd.op , this->rightDataSldPt, this->leftAttriSld) == true ){
			joinTuple(data, this->leftDataPt, this->rightDataPt, this->leftAttris, this->rightAttris);
			return 0;
		}
		else{
			// if join-focus attribute is this indexFile's attribute and cnd.op is( ==, >, >= )
			// and the current rightVal not satisfied, that indicates no need for more rightScan on indexFile. Break and next leftVal.
			if( (this->rightAttriSld.name == this->rightIn->attrName) &&
					( (this->cnd.op == EQ_OP) || (this->cnd.op == GT_OP) || (this->cnd.op == GE_OP)) ){
				break;
			}
		}
	}
	this->newRnd = true;
	return( getNextTuple(data));
}

// For attribute in vector<Attribute>, name it as rel.attr
void INLJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	for(int i=0; i< this->leftAttris.size(); i++ ) attrs.push_back(this->leftAttris[i]);
	for(int i=0; i< this->rightAttris.size(); i++ ) attrs.push_back(this->rightAttris[i]);

}


///////////////////////// Projection /////////////////////////
Project::Project(Iterator *input, const vector<string> &attrNames)
{
	this->iAttri=input;
    this->iAttri->getAttributes(attris);
    for(unsigned i = 0; i < attrNames.size(); i++)
    {
        for(vector<Attribute>::const_iterator iter = attris.begin(); iter != attris.end(); iter++)
        {
            if(iter->name == attrNames.at(i))
            {
                prjedAttris.push_back(*iter);
                break;
            }
        }
    }
}
Project::~Project()
{
	this->iAttri = NULL;
}

RC Project::getNextTuple(void *data)
{
    void *tup = malloc(PAGE_SIZE);
    memset(tup, 0, PAGE_SIZE);
    if(this->iAttri->getNextTuple(tup) != 0)
    {
        free(tup);
        return -1;
    }

    int srcNullLen = ceil( (double)this->attris.size() / 8 );
    int newNullLen = ceil( (double)this->prjedAttris.size() / 8);
    int bitNullCnt = 0;
    char byteNullIndi = 0;

    int offsetPrjedAttris = newNullLen;
    for(unsigned i = 0; i < prjedAttris.size(); i++)
    {
        int offsetAttris = srcNullLen;
        for(vector<Attribute>::const_iterator iter = attris.begin(); iter != attris.end(); iter++)
        {

            char tmpNullIndi = 0;
            memcpy(&tmpNullIndi, (char *)tup + (iter-attris.begin())/8, 1);
            if( tmpNullIndi >> ( 7 - (iter-attris.begin())%8) == 0){

				int movedOffset = sizeof(int);
				if(iter->type == TypeVarChar)
				{
					int strLength = 0;
					memcpy(&strLength, (char *)tup + offsetAttris, sizeof(int));
					movedOffset += strLength;
				}
				if(iter->name == prjedAttris.at(i).name)
				{
					byteNullIndi = byteNullIndi | (0<<(7-bitNullCnt));
					bitNullCnt ++;
					if(bitNullCnt==8 || i == prjedAttris.size()-1){
						memcpy( (char *)data +  i/8, &byteNullIndi, 1);
						bitNullCnt = 0;
						byteNullIndi = 0;
					}
					memcpy((char *)data + offsetPrjedAttris, (char *)tup + offsetAttris, movedOffset);
					offsetPrjedAttris += movedOffset;
					break;
				}
				offsetAttris += movedOffset;
            }
            else{

				if(iter->name == prjedAttris.at(i).name)
				{
					byteNullIndi = byteNullIndi | (1<<(7-bitNullCnt));
					bitNullCnt ++;
					if(bitNullCnt==8 || i == prjedAttris.size()-1){
						memcpy( (char *)data + (int)ceil(((double) i)/8), &byteNullIndi, 1);
						bitNullCnt = 0;
						byteNullIndi = 0;
					}
					break;
				}
            }
        }
    }

    free(tup);
    return 0;
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
    attrs = prjedAttris;
}


///////////////////////// Aggregation /////////////////////////
Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op)
{
	this->input=input;
	this->op=op;
	this->aggAttr=aggAttr;

}

void Aggregate::getAttributes(vector<Attribute> &attrs)const
{
	string aggregateOp;
	switch(op)
	{
	case MAX:
		aggregateOp="MAX";
		break;
	case MIN:
		aggregateOp="MIN";
		break;
	case AVG:
		aggregateOp="AVG";
		break;
	case SUM:
		aggregateOp="SUM";
		break;
	case COUNT:
		aggregateOp="COUNT";
		break;
	}
	Attribute attr;
	string words=aggregateOp = "(" + aggAttr.name + ")";
	attr.name=words;
	attr.type=aggAttr.type;
	attr.length=aggAttr.length;
	attrs.push_back(attr);

}



RC Aggregate::getNextTuple(void *data)
{
	switch(op)
	{
	case MAX:
		return this->returnMax(data);
	case MIN:
		return this->returnMin(data);
	case AVG:
		return this->returnAvg(data);
	case SUM:
		return this->returnSum(data);
	case COUNT:
		return this->returnCount(data);

	}
	return -1;
}

void Aggregate::pjWantAttrs(void* data)
{
	vector<Attribute> attrs;
	input->getAttributes(attrs);
	void* pData = data;
	void* temp = malloc(PAGE_SIZE);
	void* pTemp = temp;
	int size = 0;



	for(unsigned i = 0; i < attrs.size(); i++){
		int type=attrs[i].type;
		if(strcmp(attrs[i].name.c_str(), aggAttr.name.c_str()) == 0)
		{
			switch(type){
			case TypeInt:{
				*(int *)pTemp = *(int *)pData;

				pTemp = (char *)pTemp + sizeof(int);
				pData = (char *)pData + sizeof(int);

				size += sizeof(int);
				break;
			}
			case TypeReal:{
				*(float *)pTemp = *(float *)pData;

				pTemp = (char *)pTemp + sizeof(float);
				pData = (char *)pData + sizeof(float);

				size += sizeof(float);
				break;
			}

			}
			break;
		}
		else{
			switch(type){
			case TypeInt:
				pData = (char *)pData + sizeof(int);
				break;
			case TypeReal:
				pData = (char *)pData + sizeof(float);
				break;
			}
		}
	}

	memcpy(data,temp,size);

}


RC Aggregate::returnMax(void *max){

	void* data = malloc(200);
	input->getNextTuple(data);
	pjWantAttrs(data);
	int type=aggAttr.type;
	switch(type){
	case TypeInt:
		*(float *)max = (float)(*(int *)data);
		break;
	case TypeReal:
		*(float *)max = *(float *)data;
		break;
	}

	while(input->getNextTuple(data) != QE_EOF){
		pjWantAttrs(data);


		switch(type){
		case TypeInt:
			if(*(float *)max < (float)(*(int *)data))
			{
				*(float *)max = (float)(*(int *)data);
			}
			break;
		case TypeReal:
			if(*(float *)max < *(float *)data)
			{
				*(float *)max = *(float *)data;
			}
			break;
		}
	}


	return 0;
}

RC Aggregate::returnMin(void *min){

	void* data = malloc(200);
	input->getNextTuple(data);
	pjWantAttrs(data);
	int type=aggAttr.type;
	switch(type){
	case TypeInt:
		*(float *)min = (float)(*(int *)data);
		break;
	case TypeReal:
		*(float *)min = *(float *)data;
		break;
	}

	while(input->getNextTuple(data) != QE_EOF){
		pjWantAttrs(data);


		switch(type){
		case TypeInt:
			if(*(float *)min > (float)(*(int *)data))
			{
				*(float *)min = (float)(*(int *)data);
			}
			break;
		case TypeReal:
			if(*(float *)min > *(float *)data)
			{
				*(float *)min = *(float *)data;
			}
			break;
		}
	}


	return 0;
}

RC Aggregate::returnSum(void *sum){

	void* data = malloc(PAGE_SIZE);
	input->getNextTuple(data);
	pjWantAttrs(data);
	int type=aggAttr.type;

	switch(type){
	case TypeInt:
		*(float *)sum = (float)(*(int *)data);
		break;
	case TypeReal:
		*(float *)sum = *(float *)data;
		break;
	}

	while(input->getNextTuple(data) != QE_EOF){
		pjWantAttrs(data);


		switch(type){
		case TypeInt:
			*(float *)sum += (float)(*(int *)data);
			break;
		case TypeReal:
			*(float *)sum += *(float *)data;
			break;
		}
	}


	return 0;
}

RC Aggregate::returnAvg(void *avg){

	void* data = malloc(PAGE_SIZE);
	input->getNextTuple(data);
	pjWantAttrs(data);
	float count = 1;
	int type=aggAttr.type;
	switch(type){
	case TypeInt:
		*(float *)avg = (float)(*(int *)data);
		break;
	case TypeReal:
		*(float *)avg = *(float *)data;
		break;
	}

	while(input->getNextTuple(data) != QE_EOF)
	{
		pjWantAttrs(data);
		count++;

		switch(type){
		case TypeInt:
			*(float *)avg += (float)(*(int *)data);
			break;
		case TypeReal:
			*(float *)avg += *(float *)data;
			break;
		}
	}

	switch(type){
	case TypeInt:
		*(float *)avg = *(float *)avg/count;
		break;
	case TypeReal:
		*(float *)avg = *(float *)avg/count;
		break;
	}


	return 0;
}

RC Aggregate::returnCount(void *count){

	void* data = malloc(PAGE_SIZE);
	input->getNextTuple(data);
	*(float *)count = 1;

	while(input->getNextTuple(data) != QE_EOF){
		(*(float *)count)++;
	}

	return 0;
}

Aggregate::~Aggregate(){

}

