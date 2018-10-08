
#include "rm.h"


IndexManager* RelationManager::indexManagerPt = IndexManager::instance();
// get a nude value of a required attribute from a inserted format record srcRcrdPt
RC getSelectedAttriValFxk(void* resAttrPt, void* srcRcrdPt, vector<Attribute>& attris, Attribute targetAttri){
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

RC RelationManager::catalogDescriptionPreparation(string tableName, vector<Attribute> &attributes)
{
	string table="Tables";
	string column="Columns";
	Attribute tabAttr;
	if(table.compare(tableName)==0)
	{
		tabAttr.name="table-id";
		tabAttr.type=TypeInt;
		tabAttr.length=4;
		tabAttr.position=1;
		attributes.push_back(tabAttr);

		tabAttr.name="table-name";
		tabAttr.type=TypeVarChar;
		tabAttr.length=30;
		tabAttr.position=2;
		attributes.push_back(tabAttr);

		tabAttr.name="file-name";
		tabAttr.type=TypeVarChar;
		tabAttr.length=30;
		tabAttr.position=3;
		attributes.push_back(tabAttr);

		tabAttr.name="system-table";
		tabAttr.type=TypeInt;
		tabAttr.length=4;
		tabAttr.position=4;
		attributes.push_back(tabAttr);

		return 0;
	}

	else if(column.compare(tableName)==0)
	{
		tabAttr.name="table-id";
		tabAttr.type=TypeInt;
		tabAttr.length=4;
		tabAttr.position=1;
		attributes.push_back(tabAttr);

		tabAttr.name="column-name";
		tabAttr.type=TypeVarChar;
		tabAttr.length=50;
		tabAttr.position=2;
		attributes.push_back(tabAttr);

		tabAttr.name="column-type";
		tabAttr.type=TypeInt;
		tabAttr.length=4; //hp0
		tabAttr.position=3;
		attributes.push_back(tabAttr);

		tabAttr.name="column-length";
		tabAttr.type=TypeInt;
		tabAttr.length=4;
		tabAttr.position=4;
		attributes.push_back(tabAttr);

		tabAttr.name="column-position";
		tabAttr.type=TypeInt;
		tabAttr.length=4;
		tabAttr.position=5;
		attributes.push_back(tabAttr);

		tabAttr.name="NullFlag";
		tabAttr.type=TypeInt;
		tabAttr.length=4;
		tabAttr.position=6;
		attributes.push_back(tabAttr);

		return 0;
	}
	else
	{
		cout<<"Error!"<<endl;
		return 1;
	}
}

RC RelationManager::createTabRecord(void*data, string tableName, int tabID, string fileName, int systemTable)
{
	int offset=0;
	int tnsize=tableName.size();
	int fnsize=fileName.size();
	int nullindicator=0;

	memcpy((char*)data+offset,&nullindicator,1);
	offset+=1;

	memcpy((char*)data+offset,&tabID,sizeof(int));
	offset+=sizeof(int);

	memcpy((char*)data+offset,&tnsize,sizeof(int));
	offset+=sizeof(int);

	memcpy((char*)data+offset,tableName.c_str(),tnsize);
	offset+=tnsize;

	memcpy((char*)data+offset,&fnsize,sizeof(int));
	offset+=sizeof(int);

	memcpy((char*)data+offset,fileName.c_str(),fnsize);
	offset+=fnsize;

	memcpy((char*)data+offset,&systemTable,sizeof(int));
	offset+=sizeof(int);

	return 0;
}

RC RelationManager::createColRcd(void * data,int tabID, Attribute attr, int position, int nullflag){
	int offset=0;
	int size=attr.name.size();
	char* pnull= new char;
	memset(pnull, 0, sizeof(char));

	memcpy((char *)data+offset,pnull,1);    //null indicator
	offset+=1;
	memcpy((char *)data+offset,&tabID,sizeof(int));
	offset=offset+sizeof(int);

	memcpy((char *)data+offset,&size,sizeof(int));
	offset=offset+sizeof(int);
	memcpy((char *)data+offset,attr.name.c_str(),size);
	offset=offset+size;

	memcpy((char *)data+offset,&(attr.type),sizeof(int));
	offset=offset+sizeof(int);

	memcpy((char *)data+offset,&(attr.length),sizeof(int));
	offset=offset+sizeof(int);

	memcpy((char *)data+offset,&position,sizeof(int));
	offset=offset+sizeof(int);

	memcpy((char *)data+offset,&nullflag,sizeof(int));
	offset=offset+sizeof(int);

	return 0;

}

RC RelationManager::updateCol(int tabID,vector<Attribute> attributes)
{
	int size=attributes.size();
	RecordBasedFileManager *rbfm=RecordBasedFileManager::instance();
	FileHandle tabFileHandle;
	char* data=(char*)malloc(PAGE_SIZE);
	memset(data,0,PAGE_SIZE);
	vector<Attribute> colAttirbutes;
	RID rid;

	catalogDescriptionPreparation("Columns",colAttirbutes);
	if(rbfm->openFile("Columns", tabFileHandle)==0)
	{
		for(int i=0;i<size;i++)
		{
			createColRcd(data,tabID,attributes[i],attributes[i].position,0);
			rbfm->insertRecord(tabFileHandle,colAttirbutes,data,rid);
			rbfm->printRecord(colAttirbutes,data);
		}
		rbfm->closeFile(tabFileHandle);
		free(data);
		return 0;
	}
	free(data);
	return -1;
}

int RelationManager::getNewTabID()
{
	vector<Attribute> descriptor;
	catalogDescriptionPreparation("Tables",descriptor);
	RM_ScanIterator rm_ScanIterator;
	RID rid;
	rid.pageNum=0;
	rid.slotNum=0;
	char *data=(char *)malloc(PAGE_SIZE);
	memset(data,0,PAGE_SIZE);
	vector<string> attrname;
	attrname.push_back("table-id");
	int tabID = -1;
	int foundID= -1;

	void *x = malloc(1);
	if( scan("Tables","",NO_OP,x,attrname,rm_ScanIterator)==0 )
	{
		while(rm_ScanIterator.getNextTuple(rid,data)!=RM_EOF)
		{
			char* tmpNullInd = new char[100];
			memcpy(tmpNullInd, data, 1);
			memcpy(&foundID,(char*)data+1,sizeof(int));
			if(foundID > tabID) tabID = foundID;
		}

		free(data);
		rm_ScanIterator.close();
		free(x);
		return tabID+1;
	}
	free(data);
	free(x);
	return -1;
}

RC RelationManager::getVarChar(void *data,const string &tabName)
{
//to create a record stored: 1. the number of byte of the string; 2. the string itself
	//example
	//6Tables
	int size=tabName.size();
	int offset=0;
	memcpy((char *)data+offset,&size,sizeof(int));
	offset+=sizeof(int);
	memcpy((char *)data+offset,tabName.c_str(),size);
	return 0;
}

int RelationManager::getTabID(const string &tableName)
{
	RM_ScanIterator rm_ScanIterator;
	RID rid;
	int tabID = -1;
	char *VarChardata=(char *)malloc(PAGE_SIZE);
	char *data=(char *)malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE); //hp0
	memset(VarChardata, 0, PAGE_SIZE); //hp0
	vector<string> attrname;
	attrname.push_back("table-id");
	int count=0;
	getVarChar(VarChardata,tableName);
	if( scan("Tables","table-name",EQ_OP,VarChardata,attrname,rm_ScanIterator) == 0 )
	{
		while(rm_ScanIterator.getNextTuple(rid,data)!=RM_EOF)
		{
			memcpy(&tabID,(char *)data+1,sizeof(int));//skip null indicator
			count++;
		}
		rm_ScanIterator.close();
		free(VarChardata);
		free(data);
		return tabID;
	}
	free(VarChardata);
	free(data);
	return -1;
}

int RelationManager::vc2Str(void *data,string &toStr)
{
	int size;
	int offset=0;
	char * VarCharData=(char *) malloc(PAGE_SIZE);
	memset(VarCharData,0,PAGE_SIZE);

	memcpy(&size,(char *)data+offset,sizeof(int));
	offset+=sizeof(int);
	memcpy(VarCharData,(char *)data+offset,size);
	offset+=size;

	VarCharData[size]='\0';
	string tempstring(VarCharData);
	toStr=tempstring;
	free(VarCharData);
	return 0;
}

int RelationManager::checkSystemTab(const string &tableName)
{
	RM_ScanIterator rm_ScanIterator;
	RID rid;
	rid.pageNum=0;
	rid.slotNum=0;
	int systemtable=0;
	char *VarChardata=(char *)malloc(PAGE_SIZE);
	char *data=(char *)malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);
	memset(VarChardata, 0, PAGE_SIZE); //hp0
	vector<string> attrname;
	attrname.push_back("system-table");
	int count=0;
	getVarChar(VarChardata,tableName);
	if( scan("Tables","table-name",EQ_OP,VarChardata,attrname,rm_ScanIterator) == 0 )
	{
		while(rm_ScanIterator.getNextTuple(rid,data)!=RM_EOF)
		{
			memcpy(&systemtable,(char *)data+1,sizeof(int));
			count++;
		}
		rm_ScanIterator.close();
		free(VarChardata);
		free(data);
		return systemtable;
	}
	free(VarChardata);
	free(data);
	return -1;
}

RelationManager* RelationManager::instance()
{
    static RelationManager _rm;
    return &_rm;
}

RelationManager::RelationManager()
{
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
	FileHandle tabFileHandle;    //filehandle to handle the opened files
	RID rid;
	rid.pageNum=0;
	rid.slotNum=0;
	vector<Attribute> TabAttributes;    //information of the tables
	vector<Attribute> ColAttributes;
	if((rbfm->createFile("Tables"))==0)    //successfully create a new file named "Tables"
	{
		void* data=malloc(PAGE_SIZE);    //assign a new 4096b space data
		int systemTable=1;
		int tabID=1;

		rbfm->openFile("Tables",tabFileHandle);    //open that file and manipulate the file
		catalogDescriptionPreparation("Tables", TabAttributes);    //set the information for the tables
		createTabRecord(data, "Tables", tabID, "Tables.tbl",systemTable);    //data firstly store the primary key for the tables
		rbfm->insertRecord(tabFileHandle,TabAttributes,data,rid);    //store into the tables file and get the rid

		tabID=2;

		createTabRecord(data,"Columns",tabID,"Columns.tbl",systemTable);    //for column
		rbfm->insertRecord(tabFileHandle,TabAttributes,data,rid);    //insert again
		rbfm->closeFile(tabFileHandle);

		//create Columns
		if((rbfm->createFile("Columns"))==0)
		{
			updateCol(1,TabAttributes);
			catalogDescriptionPreparation("Columns",ColAttributes);
			updateCol(tabID,ColAttributes);
			free(data);
			data=NULL;
			return 0;
		}
		else{
			free(data);
			return -1;
		}
	}
	return -1;
}

RC RelationManager::deleteCatalog()
{
	if(rbfm->destroyFile("Tables")==0)
	{
		if(rbfm->destroyFile("Columns")==0)
		{
			return 0;
		}
	}
	return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	FileHandle filehandle;
	FileHandle nullhandle;
	vector<Attribute> tabDescriptor;
	char *data=(char *)malloc(PAGE_SIZE);
	memset(data,0,PAGE_SIZE);
	RID rid;
	rid.pageNum=0;
	rid.slotNum=0;
	int tabID=0;
	vector<Attribute> tmpattris=attrs;
	for(int i=0;i< tmpattris.size();i++)
	{
		tmpattris[i].position=i+1;
	}

		if(rbfm->createFile(tableName)==0)
		{

			if(rbfm->openFile("Tables",filehandle)==0)
			{
				tabID=getNewTabID();
				catalogDescriptionPreparation("Tables",tabDescriptor);
				createTabRecord(data,tableName,tabID,tableName,0);
				//rbfm->openFile("Tables",filehandle);
				rbfm->insertRecord(filehandle,tabDescriptor,data,rid);
				rbfm->printRecord(tabDescriptor,data);
				rbfm->closeFile(filehandle);
				if(updateCol(tabID,tmpattris)==0)
				{
					free(data);
					data=NULL;
					return 0;
				}
			}
		}

	free(data);
	data=NULL;
	return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
	FileHandle filehandle;
	RM_ScanIterator rm_ScanIterator;
	RM_ScanIterator rm_ScanIterator2;
	RID rid;
	rid.pageNum=0;
	rid.slotNum=0;
	int tabID=0;
	char *data=(char *)malloc(PAGE_SIZE);
	memset(data,0,PAGE_SIZE);
	vector<string> attrname;
	attrname.push_back("table-id");
	vector<RID> rids, ridsCol;
	vector<Attribute> tabDescriptor;
	catalogDescriptionPreparation("Tables",tabDescriptor);
	vector<Attribute> columnsdescriptor;
	catalogDescriptionPreparation("Columns",columnsdescriptor);
	if(tableName.compare("Tables") && tableName.compare("Columns"))
	{
		if(rbfm->destroyFile(tableName)==0)
		{
			tabID=getTabID(tableName);
			rbfm->openFile("Tables",filehandle);
			if(RelationManager::scan("Tables","table-id",EQ_OP,&tabID,attrname,rm_ScanIterator)==0)
			{
				while(rm_ScanIterator.getNextTuple(rid,data)!=RM_EOF)
				{
					rids.push_back(rid);
				}

				for(int j=0;j<rids.size();j++)
				{
					rbfm->deleteRecord(filehandle,tabDescriptor,rids[j]);
				}
				rbfm->closeFile(filehandle);
				rm_ScanIterator.close();
				rbfm->openFile("Columns",filehandle);
				if( scan("Columns","table-id",EQ_OP,&tabID,attrname,rm_ScanIterator2) == 0 )
				{
					while(rm_ScanIterator2.getNextTuple(rid,data)!=RM_EOF)
					{
						ridsCol.push_back(rid);
					}
					for(int j=0; j<ridsCol.size(); j++){
						rbfm->deleteRecord(filehandle,columnsdescriptor,ridsCol[j]);
					}
					rm_ScanIterator2.close();
					rbfm->closeFile(filehandle);
					free(data);
					return 0;
				}
			}
		}
	}
	free(data);
	return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	//copy the information of table with tablename to the attrs
	RM_ScanIterator rm_ScanIterator;
	RID rid;
	rid.pageNum=0;
	rid.slotNum=0;
	int tabID=0;
	char *data=(char *)malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE); //hp0
	vector<string> attrname;
	attrname.push_back("column-name");
	attrname.push_back("column-type");
	attrname.push_back("column-length");
	attrname.push_back("column-position");
	attrname.push_back("NullFlag");
	Attribute attr;
	string tempstr;
	int nullflag=0;
	int offset = 0;
	tabID=getTabID(tableName);  // scan table ID
	if( tabID == -1 ) return -1;
	if( scan("Columns","table-id",EQ_OP,&tabID,attrname,rm_ScanIterator) == 0 )
	{
		while(rm_ScanIterator.getNextTuple(rid,data)!=RM_EOF)
		{
			offset=1;    //go to the byte after the null indicator
			vc2Str(data+offset,tempstr);
			attr.name=tempstr;
			offset+=(sizeof(int)+tempstr.size());
			memcpy(&(attr.type),data+offset,sizeof(int));
			offset+=sizeof(int);
			memcpy(&(attr.length),data+offset,sizeof(int));
			offset+=sizeof(int);
			memcpy(&(attr.position),data+offset,sizeof(int));
			offset+=sizeof(int);
			memcpy(&(nullflag),data+offset,sizeof(int));
			offset+=sizeof(int);
			if(nullflag==1)
			{
				attr.length=0;
			}
			attrs.push_back(attr);
		}
		rm_ScanIterator.close();
		free(data);
		return 0;
	}
	free(data);
	return -1;
}

RC RelationManager::insertTupleWithIndex(FileHandle& fileHandle, vector<Attribute>& descriptor, const void* data, RID rid){

	void* indexData = malloc(PAGE_SIZE);
	char nullFlag;
	string tableName = fileHandle.fileName;
	RC rc;
	char* realDataPt = (char*)data + (int)ceil((float)(descriptor.size())/8);
	for(int i=0; i<descriptor.size(); i++){
		memset(indexData, 0, PAGE_SIZE);
		nullFlag = 0;
		memcpy(&nullFlag, (char*)data + i/8, 1);
		int nullBitFlag = nullFlag&(1<<(7-i%8));
		if(nullBitFlag==0){

			if(descriptor[i].type==TypeVarChar){
				int tmpDataLen = 0;
				memcpy(&tmpDataLen, realDataPt, sizeof(int));
				memcpy(indexData, realDataPt, sizeof(int) + tmpDataLen);
				realDataPt += sizeof(int) + tmpDataLen;
			}
			else{
				memcpy(indexData, realDataPt, 4);
				realDataPt += 4;
			}

			string tmpIndexFileName = tableName + "_" + descriptor[i].name;
			IXFileHandle ixfileHandle;
			rc = indexManagerPt->openFile(tmpIndexFileName, ixfileHandle);
			if(rc==0){
				rc = indexManagerPt->insertEntry(ixfileHandle, descriptor[i], indexData, rid);
				///////////////////
				//cout<<"insert "<<ixfileHandle.fileHandle.fileName<<" "<<*(int*)indexData<<" "<<rid.pageNum<<" "<<rid.slotNum<<endl;
				//////////////////
				rc = indexManagerPt->closeFile(ixfileHandle);

			}
		}
	}

	free(indexData);
	return 0;
}

RC RelationManager::deleteTupleWithIndex(FileHandle& fileHandle, vector<Attribute>& descriptor, RID rid){

	void* data = malloc(PAGE_SIZE);
	void* indexData = malloc(PAGE_SIZE);
	char nullFlag;
	memset(data, 0, PAGE_SIZE);
	string tableName = fileHandle.fileName;
	RC rc;
	readTuple(tableName, rid, data);
	char* realDataPt = (char*)data + (int)ceil((float)(descriptor.size())/8);
	for(int i=0; i<descriptor.size(); i++){
		memset(indexData, 0, PAGE_SIZE);
		nullFlag = 0;
		memcpy(&nullFlag, (char*)data + i/8, 1);
		int nullBitFlag = nullFlag&(1<<(7-i%8));
		if(nullBitFlag==0){

			if(descriptor[i].type==TypeVarChar){
				int tmpDataLen = 0;
				memcpy(&tmpDataLen, realDataPt, sizeof(int));
				memcpy(indexData, realDataPt, sizeof(int) + tmpDataLen);
				realDataPt += sizeof(int) + tmpDataLen;
			}
			else{
				memcpy(indexData, realDataPt, 4);
				realDataPt += 4;
			}

			string tmpIndexFileName = tableName + "_" + descriptor[i].name;
			IXFileHandle ixfileHandle;
			rc = indexManagerPt->openFile(tmpIndexFileName, ixfileHandle);
			if(rc==0){
				rc = indexManagerPt->deleteEntry(ixfileHandle, descriptor[i], indexData, rid);
				///////////////////
				//cout<<"delete "<<ixfileHandle.fileHandle.fileName<<" "<<*(int*)indexData<<" "<<rid.pageNum<<" "<<rid.slotNum<<endl;
				//////////////////
				rc = indexManagerPt->closeFile(ixfileHandle);

			}
		}
	}

	free(indexData);
	free(data);
	return 0;
}

RC RelationManager::updateTupleWithIndex(FileHandle& fileHandle, vector<Attribute>& descriptor, const void* upData, RID rid){

	deleteTupleWithIndex(fileHandle, descriptor, rid);
	insertTupleWithIndex(fileHandle, descriptor, upData, rid);
	return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	FileHandle filehandle;
	vector<Attribute> descriptor;
	RC rc;
	if(checkSystemTab(tableName)==0)
	{
		RelationManager::getAttributes(tableName,descriptor);
		rbfm->openFile(tableName,filehandle);
		if(filehandle.fileHandlePf != NULL)
		{
			if(rbfm->insertRecord(filehandle,descriptor,data,rid)==0)
			{
				insertTupleWithIndex(filehandle, descriptor, data, rid);
				rbfm->closeFile(filehandle);
				return 0;
			}
		}
	}
	return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	FileHandle filehandle;
	vector<Attribute> descriptor;
	RC rc;

	if(checkSystemTab(tableName)==0)
	{
		RelationManager::getAttributes(tableName,descriptor);
		rbfm->openFile(tableName,filehandle);
		if(filehandle.fileHandlePf != NULL)
		{
			if(rbfm->deleteRecord(filehandle,descriptor,rid)==0)
			{
				deleteTupleWithIndex(filehandle, descriptor, rid);
				rbfm->closeFile(filehandle);
				return 0;
			}
		}
	 }

	return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	FileHandle filehandle;
	vector<Attribute> descriptor;
	void* deleteData = malloc(PAGE_SIZE);
	void* insertData = malloc(PAGE_SIZE);
	void* indexData = malloc(PAGE_SIZE);
	int nullFlag = 0;
	RC rc = 0;
	if(checkSystemTab(tableName)==0)
	{
		getAttributes(tableName,descriptor);
		rbfm->openFile(tableName,filehandle);
		if(filehandle.fileHandlePf != NULL)
		{
			if(rbfm->updateRecord(filehandle,descriptor,data,rid)==0)
			{
				updateTupleWithIndex(filehandle, descriptor, data, rid);
				rbfm->closeFile(filehandle);
				return 0;
			}
		}
	}
	return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	FileHandle filehandle;
	vector<Attribute> descriptor;
	getAttributes(tableName,descriptor);
	rbfm->openFile(tableName,filehandle);
	if(filehandle.fileHandlePf != NULL)
	{
		if(rbfm->readRecord(filehandle,descriptor,rid,data)==0)
		{
			rbfm->closeFile(filehandle);
			return 0;
		}
	}
	return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	if(rbfm->printRecord(attrs,data)==0)
	{
		return 0;
	}
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    FileHandle fileHandle;
    vector<Attribute> descriptor;
    getAttributes(tableName,descriptor);
    rbfm->openFile(tableName,fileHandle);
	if(fileHandle.fileHandlePf != NULL)
    {
    	if(rbfm->readAttribute(fileHandle,descriptor,rid,attributeName,data)==0)
    	{
    		rbfm->closeFile(fileHandle);
    		return 0;
    	}
    }
    return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,
      const void *value,
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{

	//use rbfm
	FileHandle filehandle;
	vector<Attribute> descriptor;
	if(tableName.compare("Tables")==0)
	{
		catalogDescriptionPreparation("Tables",descriptor);
	}

	else if(tableName.compare("Columns")==0)
	{
		catalogDescriptionPreparation("Columns",descriptor);
	}

	else
	{
		RelationManager::getAttributes(tableName,descriptor);
	}
	rbfm->openFile(tableName,filehandle);
	if(filehandle.fileHandlePf != NULL)
	{
		if(rbfm->scan(filehandle,descriptor,conditionAttribute,compOp,value,attributeNames,rm_ScanIterator.rbfm_ScanIterator,rbfm)==0)
		{
			return 0;
		}
	}
	return -1;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
	return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
	vector<Attribute> descriptor;
	int pos=0;
	int tabID=0;
	vector<Attribute> tmpattr;
	tmpattr.push_back(attr);
	tabID=getTabID(tableName);

	getAttributes(tableName,descriptor);
	pos=(descriptor.back()).position +1;
	tmpattr[0].position=pos;
	if(updateCol(tabID,tmpattr)==0)
	{
		return 0;
	}
	return -1;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
{
	return rbfm_ScanIterator.getNextRecord(rid,data);
}


RC RelationManager::createIndex(const string &tableName, const string &attributeName){

    RID rid;
    void *returnedData = malloc(PAGE_SIZE);
	memset(returnedData, 0, PAGE_SIZE);
    IXFileHandle ixfileHandle;
    Attribute attributeSld;

    string ixFileName = tableName + "_" + attributeName;
    RC rc = indexManagerPt->createFile(ixFileName);
    rc = indexManagerPt->openFile(ixFileName, ixfileHandle);

    vector<Attribute> attrsAll;
    rc = getAttributes(tableName, attrsAll);
    for(int i=0; i<attrsAll.size(); i++){
    	if(attrsAll[i].name == attributeName) attributeSld = attrsAll[i];
    }

    RM_ScanIterator rmsi;
    vector<string> attrs;
    attrs.push_back(attributeName);
    rc = scan(tableName, "", NO_OP, NULL, attrs, rmsi);
    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF){
    	char nullByte = 0;
    	memcpy(&nullByte, returnedData, 1);
    	if((nullByte>>7) == 0){
			rc = indexManagerPt->insertEntry(ixfileHandle, attributeSld, (char*)returnedData + 1, rid);
    	}
		memset(returnedData, 0, PAGE_SIZE);
    }
    free(returnedData);
    rmsi.close();
    indexManagerPt->closeFile(ixfileHandle);
    return 0;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName){
	vector<Attribute> attrsAll;
	for(int i=0; i<attrsAll.size(); i++){
		string indexFileName = tableName + "_" + attrsAll[i].name;
		indexManagerPt->destroyFile(indexFileName);
	}
	return 0;
}

// indexScan returns an iterator to allow the caller to go through qualified entries in index
RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator){


    vector<Attribute> attrsAll;
    RC rc = getAttributes(tableName, attrsAll);
    for(int i=0; i<attrsAll.size(); i++){
    	if(attrsAll[i].name == attributeName) this->IXattribute = attrsAll[i];
    }

    string indexFileName = tableName + "_" + attributeName;

    rc = indexManagerPt->openFile(indexFileName, this->ixfileHandle);
    rc = indexManagerPt->scan(this->ixfileHandle, this->IXattribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_ScanIterator);
    return rc;
}


RM_IndexScanIterator::RM_IndexScanIterator(){
	// Constructor
}
RM_IndexScanIterator::~RM_IndexScanIterator(){
	// Destructor
}

// "key" follows the same format as in IndexManager::insertEntry()
RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
	// Get next matching entry
	return this->ix_ScanIterator.getNextEntry(rid, key);
}
RC RM_IndexScanIterator::close(){
	return this->ix_ScanIterator.close();
}
