
%let Oldseg = "D:\Repos\QC_NRN\nrn_rrn_sk_shp_en\NRN_SK_9_0_ROADSEG.dbf";
%let OldStrnme = "D:\Repos\QC_NRN\nrn_rrn_sk_shp_en\NRN_SK_9_0_STRPLANAME.dbf";
%let OldAddRng = "D:\Repos\QC_NRN\nrn_rrn_sk_shp_en\NRN_SK_9_0_ADDRANGE.dbf";

%let Newseg = "D:\Repos\QC_NRN\nrn_shp_new\ROADSEG.dbf";
%let NewStrnme = "D:\Repos\QC_NRN\nrn_shp_new\STRPLANAME.dbf";
%let NewAddRng = "D:\Repos\QC_NRN\nrn_shp_new\ADDRANGE.dbf";

%let report_loc = D:\Repos\QC_NRN\QC_log\;


OPTIONS FORMCHAR="|----|+|---+=|-/\<>*";

%macro sexyimport(mysource,myoutput);
	Proc import
		datafile=&mysource
		dbms=DBF
		out= &myoutput
		Replace;
	Run;
%mend;

%sexyimport(&Oldseg,Previous_ROADSEG);
%sexyimport(&OldStrnme,Previous_STRPLANAME);
%sexyimport(&OldAddRng,Previous_ADDRANGE);

%sexyimport(&Newseg,New_ROADSEG);
%sexyimport(&NewStrnme,New_STRPLANAME);
%sexyimport(&NewAddRng,New_ADDRANGE);


/*Table count*/
proc sql;
	create table table_count as
		select memname,nobs from dictionary.tables 
		where upcase(libname)='WORK' 
		and lowcase(Memname) in ('previous_roadseg','previous_strplaname','previous_addrange','new_roadseg','new_strplaname','new_addrange');
quit;


/*Table structure*/
%macro sexycontents(mysource,myoutput);
Proc contents
	data=&mysource.
	memtype=data
	noprint
	out= &myoutput.(keep=name type length format);
Run;
%mend;

%sexycontents(Previous_ROADSEG, P_ROADSEG_STRUCTURE);
%sexycontents(Previous_STRPLANAME,P_STRPLANAME_STRUCTURE);
%sexycontents(Previous_ADDRANGE,P_ADDRANGE_STRUCTURE);
%sexycontents(New_ROADSEG, N_ROADSEG_STRUCTURE);
%sexycontents(New_STRPLANAME,N_STRPLANAME_STRUCTURE);
%sexycontents(New_ADDRANGE,N_ADDRANGE_STRUCTURE);


%macro sexystructurecompare(thenew,theold);
	Proc sort
		data=&theold;
		by _ALL_;
	Run;
	Proc sort
		data=&thenew;
		by _ALL_;
	Run;

	ods listing;
	    proc printto print="&report_loc.&thenew..txt" new;
	    run;

	    proc compare
	    	base= &thenew
	    	compare= &theold novalues;
	    run;

	    proc printto; 
	    run;
	ods listing close;

%mend;

%sexystructurecompare(N_ROADSEG_STRUCTURE,P_ROADSEG_STRUCTURE);
%sexystructurecompare(N_STRPLANAME_STRUCTURE,P_STRPLANAME_STRUCTURE);
%sexystructurecompare(N_ADDRANGE_STRUCTURE,P_ADDRANGE_STRUCTURE);

/*Verify NID match*/
/*Fun with join*/
/*Expected 0, No NID from addrange should be orphan, adrangenid from roadseg should all match to an ADDRANGE*/


ods listing;
Proc SQL;
	Select count(NID)into: orphan_ADDRANGE_NID
	From new_addrange
	Where NID NOT IN
		(Select ADRANGENID From new_roadseg);

	Select count(ADRANGENID)into: orphan_ROADSEG_ADRANGENID
	From new_roadseg
	Where ADRANGENID NOT IN
		(Select NID From new_addrange);

	Select count(L_OFFNANID)into:orphan_L_OFFNANID
	From new_addrange
	Where L_OFFNANID NOT IN
		(Select NID From new_strplaname);

	Select count(R_OFFNANID)into:orphan_R_OFFNANID
	From new_addrange
	Where R_OFFNANID NOT IN
		(Select NID From new_strplaname);

	Select count(NID)into:orphan_STRPLANAME_NID
	From new_strplaname 
	Where (NID NOT IN
		(Select L_OFFNANID  From new_addrange))
	and (NID NOT IN
		(Select R_OFFNANID  From new_addrange));

Quit;

%put &orphan_ADDRANGE_NID;
%put &orphan_ROADSEG_ADRANGENID;
%put &orphan_L_OFFNANID;
%put &orphan_R_OFFNANID;
%put &orphan_STRPLANAME_NID;

%macro orphan_NID;

%IF &orphan_ADDRANGE_NID > 0 %THEN
	%DO; 
	Proc SQL;
		Create table orphan_ADDRANGE_NID as
			Select NID
			From new_addrange
			Where NID NOT IN
				(Select ADRANGENID From new_roadseg);	
	Quit;
	
	Proc export
		Data= orphan_ADDRANGE_NID
		Dbms= XLSX
		Outfile="&report_loc.orphan_ADDRANGE_NID.xlsx"
		label
		Replace;
		Sheet='Sheet1';
	Run;
	%END;

%IF &orphan_ROADSEG_ADRANGENID > 0 %THEN
	%DO; 
	Proc SQL;
		Create table orphan_ROADSEG_ADRANGENID as
			Select ADRANGENID
			From new_roadseg
			Where ADRANGENID NOT IN
				(Select NID From new_addrange);
	Quit;

	Proc export
		Data= orphan_ROADSEG_ADRANGENID
		Dbms= XLSX
		Outfile="&report_loc.orphan_ROADSEG_ADRANGENID.xlsx"
		label
		Replace;
		Sheet='Sheet1';
	Run;
	%END;

%mend;
%orphan_NID;


/*Compare Old and New NID*/
/*Compare Data where NID stayed the same*/


%Macro NIDCHANGE(table);
Proc SQL;
	Create table &table._NID_old as
	Select distinct NID 
	from Previous_&table.
	order by NID;

	Create table &table._NID_new as
	Select distinct NID 
	from New_&table.
	order by NID;

	Create table &table._MATCH_NID as
	select B.*
	from &table._NID_old A, &table._NID_new B
	where A.NID=B.NID;

	Select count(NID)into: dropped_&table._NID
	From &table._NID_old
	Where NID NOT IN
		(Select NID From &table._NID_new);

	Select count(NID)into: new_&table._NID
	From &table._NID_new
	Where NID NOT IN
		(Select NID From &table._NID_old);

	%put Dropped NID:&&dropped_&table._NID;
	%put New NID:&&new_&table._NID ;
Quit;
%mend;

%NIDCHANGE(ROADSEG);
%NIDCHANGE(ADDRANGE);
%NIDCHANGE(STRPLANAME);

%macro sexycompareattribute(table);
	Proc SQL;
		Create table &table._match_old as
			select *
			from Previous_&table.
			where NID in (Select NID from New_&table.)
			order by NID;
		Create table &table._match_new as
			select  *
			from New_&table.
			where NID in (Select NID from Previous_&table.)
			order by NID;
	Quit;

	%IF (&table.=ADDRANGE or &table.=STRPLANAME) %THEN
	%DO; 
		ods listing;
		    proc printto print="&report_loc.&table._attributes.txt" new;
		    run;

		    proc compare
		    	base= &table._match_new
		    	compare= &table._match_old novalues;
		    run;

		    proc printto; 
		    run;
		ods listing close;
	%END;
	%IF (&table.=ROADSEG) %THEN
	%DO; 
		ods listing;
		    proc printto print="&report_loc.&table._attributes.txt" new;
		    run;

		    proc compare
		    	base= &table._match_new
		    	compare= &table._match_old novalues;
				ID NID;
		    run;

		    proc printto; 
		    run;
		ods listing close;
	%END;
%mend;

%sexycompareattribute(ROADSEG);
%sexycompareattribute(ADDRANGE);
%sexycompareattribute(STRPLANAME);


/*interesting... some ADDRANGE can be matched to more than one records on ROADSEG
SK have no Address data but if they had, that would likely mean overlaps*/
/*
Proc SQL;
	Select count(distinct ADRANGENID)
	From new_roadseg;
Quit;*/



/*test*/

