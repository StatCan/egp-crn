* ====================================================================================
    Backgound: 
        A multiparty agreement has been signed between NRCan (GeoBase), EC, StatCan, 
    and some provinces and territories (NRNv2 Agreement) to get geographic data 
    that could be integrated to the NGD.
    According to the NRNv2 Agreement, the funds that StatCan and EC are making 
    available to provinces and territories are paid (pro-rated) according to 
    the rate of named and addressed road segments.
  
    Purpose: Generate annual stats base on data deliveries (downloaded from a FTP site)
             from prov under the Agrement

    Original Author : R. Dunphy
    Last modified by: F. Pierre, March 2011
					  R. Dunphy, February 2012 ... ON dbf is ugly to get to the directory:
 						V:\DataManagement\External_Data_Source\Provincial_data_source\ON\LIO_06Feb2012\sc21-lio-2012-02-06-141948-893365\spatial\ornsegad
						either move the data or set a parameter for read-in
						Also - macro invocation? %macro NRNV2_STAT(delivery=NS\ns_nrn_2010_2011_delivery1)?
						- LAST AB data has all table prefaced with ABRN_*, so change in import, road class (reclass text to number), addressing (text to number)
					  M. Laforme, March 2017 
 ========================================================================================;

* ==========================================================================================
    Step 1: Provincial Data prepaparation (differ by prov, may differ delivery to delivery)
            
    Step 2: generate the stat and output and Excel sheet

    Output: &Path\Reports\External_sources_Stats_for_NRNV2_Agreements_Current.XLS, 
            sheet=&PR_OUT1

    !!! To do after the execution of this PGM: 
    Copy and paste &PR_OUT1 sheet to &PR_OUT to update NRNV2 sheet
 ============================================================================================;

* ========================== Step 0: Set-up directories, macro, variables ==========================;
%let Path=V:\DataManagement\External_Data_Source\Prov_Data_Eval_Metrics;
*%let Pathdata=V:\DataManagement\Prov_Data_Eval_Metrics\DATA;
*%let Pathdata=V:\DataManagement\External_Data_Source\Provincial_data_source;
%let Pathdata=D:\Repos\QC_NRN\;
libname wkspc "&Path.\SAS_Data";

* --- Include pgm to calculate STATs ----;
%include "&path\Scripts\gen_pr_metrics.sas";

* --- Variable name, WIP, only implemented for AB,PEI,NS rest still hardcoded ----;
%let Roadseg=ROADSEG.dbf;
%let Addrange=ADDRANGE.dbf;
%let STRPLANAME=STRPLANAME.dbf;

%let expath= D:\Repos\QC_NRN\SK_Stats_for_NRNV2_Mar2019.xlsx;
%let resultpath= %sysfunc(cats("&expath"));



%macro NRNV2_STAT(delivery/*=AB\2017\NRN_AB_16_0_SHAPE_en*/);
 
    %global PR;
    %let PR=%SUBSTR(&delivery.,1,2);

* ==========================================================================================
        Step 1 : Read and merge ROADSEG, ADDRANGE, and STRPLANAME (if necessary)
  ==========================================================================================;
%if &PR=PE %then %do; * because not following NRCAN model;
*seems the dbf name not always ROADSEG_OUT, can change with new deliveries e.g. PEI_NRN11.dbf;
*2015 - PEI delivery now follows NRN 3 file convention;
    proc import datafile="&Pathdata.\&delivery.\&ROADSEG" 
        out=wkspc.&PR._ROADSEG dbms=dbf replace;
    proc import datafile="&Pathdata.\&delivery.\&ADDRANGE"
        out=wkspc.&PR._ADDRANGE dbms=dbf replace;
    proc import
        datafile="&Pathdata.\&delivery.\&STRPLANAME"
        out=wkspc.&PR._STRPLANAME dbms=dbf replace;
    run;

    proc sql;
    create table wkspc.&PR._data as
    select  a.roadsegid as PRRDID,
            a.roadclass as PRCLASS1,
            b.l_hnumf as PR_FR_L1,
            b.l_hnuml as PR_TO_L1,
            b.r_hnumf as PR_FR_R1,
            b.r_hnuml as PR_TO_R1,
            c.namebody as PRSTRNME
    from wkspc.&PR._ROADSEG a
    left join wkspc.&PR._ADDRANGE b
        on a.ADRANGENID=b.NID
    left join wkspc.&PR._STRPLANAME c
        on b.L_OFFNANID=c.NID;
    quit;

	* ----- Recode ROADCLASS to NRN, convert text addressing to numeric ----;
	data wkspc.&PR._data (keep=PRRDID PRCLASS PR_FR_L PR_TO_L PR_FR_R PR_TO_R PRSTRNME);
	set wkspc.&PR._data;
	format PR_FR_L PR_TO_L PR_FR_R PR_TO_R 6.;
	if      COMPRESS(LOWCASE(PRCLASS1))='alleyway/lane'       then PRCLASS=8;
	else if COMPRESS(LOWCASE(PRCLASS1))='arterial'            then PRCLASS=3;
	else if COMPRESS(LOWCASE(PRCLASS1))='collector'           then PRCLASS=4;
	else if COMPRESS(LOWCASE(PRCLASS1))='freeway'  			  then PRCLASS=1;
	else if COMPRESS(LOWCASE(PRCLASS1))='expressway/highway'  then PRCLASS=2;
	else if COMPRESS(LOWCASE(PRCLASS1))='local/street'        then PRCLASS=5;
	else if COMPRESS(LOWCASE(PRCLASS1))='ramp'                then PRCLASS=9;
	else if COMPRESS(LOWCASE(PRCLASS1))='resource/recreation' then PRCLASS=10;
	else if COMPRESS(LOWCASE(PRCLASS1))='rapidtransit'        then PRCLASS=11;
	else if COMPRESS(LOWCASE(PRCLASS1))='servicelane'         then PRCLASS=12;
	else if COMPRESS(LOWCASE(PRCLASS1))='winter'              then PRCLASS=13;
	else if COMPRESS(LOWCASE(PRCLASS1))='local/strata'        then PRCLASS=6;
	else if COMPRESS(LOWCASE(PRCLASS1))='local/unknown'       then PRCLASS=7;
	else                                                             PRCLASS=0;
    PR_FR_L=PR_FR_L1;
    PR_TO_L=PR_TO_L1;
    PR_FR_R=PR_FR_R1;
    PR_TO_R=PR_TO_R1;
	run;

 /*   proc import datafile="&Pathdata.\&delivery.\NRN_PE_14_0_ROADSEG.dbf" 
        out=wkspc.&PR._ROADSEG dbms=dbf replace;
    run;

    proc sql;
    create table wkspc.&PR._data as
    select  nid as PRRDID,
            roadclass as PRCLASS,
            L_HNUMF as PR_FR_L,
            L_HNUML as PR_TO_L,
            R_HNUMF as PR_FR_R,
            R_HNUML as PR_TO_R,
            NAME as PRSTRNME
    from wkspc.&PR._ROADSEG;
    quit;

    %let PRRDID=NID;
    %let PRCLASS=ROADCLASS;
    %let PR_FR_L=L_HNUMF;
    %let PR_TO_L=L_HNUML;
    %let PR_FR_R=R_HNUMF;
    %let PR_TO_R=R_HNUML;
    %let PRSTRNME=NAME;

    /* set view */
/*    proc access dbms=dbf;
    create wkspc.&PR._data.access;
    path="&Pathdata.\&delivery.\ROADSEG.dbf"; *seems the dbf name not always ROADSEG_OUT, can change with new deliveries e.g. PEI_NRN11.dbf;

    create wkspc.&PR._data.view;
    select &PRRDID &PRCLASS
           &PR_FR_L &PR_TO_L
           &PR_FR_R &PR_TO_R
           &PRSTRNME;
    rename &PRRDID=PRRDID &PRCLASS=PRCLASS
           &PR_FR_L=PR_FR_L &PR_TO_L=PR_TO_L
           &PR_FR_R=PR_FR_R &PR_TO_R=PR_TO_R
           &PRSTRNME=PRSTRNME;
    run; 
*/

%end;

%else %if &PR=NB %then %do; * because not following NRCAN model;
/* 2015 delivery fgdb, export to .shp */

    proc import datafile="&Pathdata.\&delivery.\RoadSegmentEntity.dbf"
         out=wkspc.&PR._data dbms=dbf replace;
    run; 

    * ---- None None / UNKNOWN is unnamed (Rob, 11/03/2014) ----;
    data wkspc.&PR._data; 
	set wkspc.&PR._data;
    if compress(UPCASE(Left_Stree))="NONENONE" then Left_Stree="";
    if FIND(Left_Stree,"UNKNOWN")>0 then Left_Stree="";  
    run;

    proc sql;
    create table wkspc.&PR._data as
    select  a.road_segme as PRRDID,
    case 
		when a.Func_Road_ = 'Arterial' then 3
		when a.Func_Road_ = 'Collector' then 4
		when a.Func_Road_ = 'DOT Local Named (Gravel)' then 5
		when a.Func_Road_ = 'Expressway/highway' then 2
		when a.Func_Road_ = 'Freeway' then 1
		when a.Func_Road_ = 'Local/street' then 5
		when a.Func_Road_ = 'Local/unknown' then 7
		when a.Func_Road_ = 'NBDNR Resource Road F1' then 10
		when a.Func_Road_ = 'NBDNR Resource Road F2' then 10
		when a.Func_Road_ = 'NBDNR Resource Road F3' then 10
		when a.Func_Road_ = 'NBDNR Resource Road F4' then 10
		when a.Func_Road_ = 'NBDNR Resource Road F5' then 10
		when a.Func_Road_ = 'NBDNR Resource Road F6' then 10
		when a.Func_Road_ = 'NBDOT Local Named' then 5
		when a.Func_Road_ = 'NBDOT Local Numbered' then 5
		when a.Func_Road_ = 'NBDOT Road Public Access' then 5
		when a.Func_Road_ = 'Ramp' then 9
		when a.Func_Road_ = 'Service Lane' then 12
		when a.Func_Road_ = 'Weigh Station' then 12
		else 0
   end as PRCLASS,
   input(a.Left_First,8.) as PR_FR_L,
   input(a.Left_Last_,8.) as PR_TO_L,
   input(a.Right_Firs,8.) as PR_FR_R,
   input(a.Right_Last,8.) as PR_TO_R,
   a.Left_Stree as PRSTRNME
    from wkspc.&PR._data a;
    quit;
%end; 


%else %if &PR=QC %then %do; * because not following NRCAN model - 2016 export from fgdb to dbf;

    /*proc import datafile="&Pathdata.\&delivery.\AQ_2016.dbf"
         out=wkspc.&PR._data dbms=dbf replace;
    run; */

    * ---- Voie seem to be an artefat for un-named (to be confirmed with QC rep) (Fritz, 20/03/2011) ----;
	* ---- Change in var names with new fgdb delivery (Rob 09/09/2016)----;
    /*data wkspc.QC_data; 
	set wkspc.QC_data;
    if compress(UPCASE(DRODORECLG))="VOIE" then DRODORECLG=""; 
    run;*/

    proc sql;
    create table wkspc.&PR._data as
    select  a.IDRTE as PRRDID,
    case when a.caractrte = 'bretelle' then 9
         when a.CLSRTE = 'Accès aux resources et aux localités isolées' then 10
         when a.CLSRTE = 'Accès ressources' then 10
         when a.CLSRTE = 'Autoroute' then 2
         when a.CLSRTE = 'Collectrice municipale' then 4
         when a.CLSRTE = 'Artère' then 3
         when a.CLSRTE = 'Nationale' then 1
         when a.CLSRTE = 'Locale' then 5
         when a.CLSRTE = 'Régionale' then 3
		 when a.CLSRTE = 'Collectrice de transit' then 11
            else 10
            end as PRCLASS,
            a.GAMINADR as PR_FR_L,
            a.GAMAXADR as PR_TO_L,
            a.DRMINADR as PR_FR_R,
            a.DRMAXADR as PR_TO_R,
            a.DRODORECLG as PRSTRNME
    from wkspc.&PR._data a;
    quit;
%end; 

/* There are sometime NRN deliveries of ON in the NRCAN model - if so, comment this LIO version out to default to NRN data prep*/
/*%else %if &PR=ON %then %do; 

    proc import datafile="&Pathdata.\&delivery.\ORN_Project.dbf"
         out=wkspc.ON_data dbms=dbf replace;
    run; 

	* ----- Same codificationas ----;
	data wkspc.ON_data (keep=PRRDID PRCLASS PR_FR_L PR_TO_L PR_FM_R PR_TO_R PRSTRNME);
	set wkspc.ON_data;
	if      COMPRESS(LOWCASE(road_class))='alleyway/lane'       then rd_class_tmp=8;
	else if COMPRESS(LOWCASE(road_class))='arterial'            then rd_class_tmp=3;
	else if COMPRESS(LOWCASE(road_class))='collector'           then rd_class_tmp=4;
	else if COMPRESS(LOWCASE(road_class))='expressway/highway'  then rd_class_tmp=2;
	else if COMPRESS(LOWCASE(road_class))='local/street'        then rd_class_tmp=5;
	else if COMPRESS(LOWCASE(road_class))='ramp'                then rd_class_tmp=9;
	else if COMPRESS(LOWCASE(road_class))='resource/recreation' then rd_class_tmp=10;
	else if COMPRESS(LOWCASE(road_class))='rapidtransit'        then rd_class_tmp=11;
	else if COMPRESS(LOWCASE(road_class))='service'             then rd_class_tmp=12;
	else if COMPRESS(LOWCASE(road_class))='winter'              then rd_class_tmp=10;
	else if COMPRESS(LOWCASE(road_class))='local/strata'        then rd_class_tmp=6;
	else                                                             rd_class_tmp=10;
	if L_FIRST=-1 then L_FIRST=0; if L_LAST=-1 then L_LAST=0;
	if R_FIRST=-1 then R_FIRST=0; if R_LAST=-1 then R_LAST=0;
	PRRDID=OBJECT_ID;
	PRCLASS=road_class_tmp;
	PR_FR_L=L_FIRST;
	PR_TO_L=L_LAST;
	PR_FM_R=R_FIRST;
	PR_TO_R=R_LAST;
	PRSTRNME=name_body;
	run;
%end; 
*/
/* AB switched delivery specs from NRN to STC specific*/
%else %if &PR=AB %then %do; 

    proc import datafile="&Pathdata.\&delivery.\&ROADSEG" 
        out=wkspc.&PR._ROADSEG dbms=dbf replace;
    proc import datafile="&Pathdata.\&delivery.\&ADDRANGE"
        out=wkspc.&PR._ADDRANGE dbms=dbf replace;
    proc import
        datafile="&Pathdata.\&delivery.\&STRPLANAME"
        out=wkspc.&PR._STRPLANAME dbms=dbf replace;
    run;

	proc sql;
    create table wkspc.&PR._data as
    select  a.roadsegid as PRRDID,
            a.roadclass as PRCLASS1,
            b.l_hnumf as PR_FR_L1,
            b.l_hnuml as PR_TO_L1,
            b.r_hnumf as PR_FR_R1,
            b.r_hnuml as PR_TO_R1,
            c.namebody as PRSTRNME
    from wkspc.&PR._ROADSEG a
    left join wkspc.&PR._ADDRANGE b
        on a.ADRANGENID=b.NID
    left join wkspc.&PR._STRPLANAME c
        on b.L_OFFNANID=c.NID;
    quit;
	* ----- Recode ROADCLASS to NRN, convert text addressing to numeric ----;
	data wkspc.&PR._data (keep=PRRDID PRCLASS PR_FR_L PR_TO_L PR_FR_R PR_TO_R PRSTRNME);
	set wkspc.&PR._data;
	format PR_FR_L PR_TO_L PR_FR_R PR_TO_R 6.;
	if      COMPRESS(LOWCASE(PRCLASS1))='alleyway/lane'       then PRCLASS=8;
	else if COMPRESS(LOWCASE(PRCLASS1))='arterial'            then PRCLASS=3;
	else if COMPRESS(LOWCASE(PRCLASS1))='collector'           then PRCLASS=4;
	else if COMPRESS(LOWCASE(PRCLASS1))='freeway'  			  then PRCLASS=1;
	else if COMPRESS(LOWCASE(PRCLASS1))='expressway/highway'  then PRCLASS=2;
	else if COMPRESS(LOWCASE(PRCLASS1))='local/street'        then PRCLASS=5;
	else if COMPRESS(LOWCASE(PRCLASS1))='ramp'                then PRCLASS=9;
	else if COMPRESS(LOWCASE(PRCLASS1))='resource/recreation' then PRCLASS=10;
	else if COMPRESS(LOWCASE(PRCLASS1))='rapidtransit'        then PRCLASS=11;
	else if COMPRESS(LOWCASE(PRCLASS1))='servicelane'         then PRCLASS=12;
	else if COMPRESS(LOWCASE(PRCLASS1))='winter'              then PRCLASS=13;
	else if COMPRESS(LOWCASE(PRCLASS1))='local/strata'        then PRCLASS=6;
	else if COMPRESS(LOWCASE(PRCLASS1))='local/unknown'       then PRCLASS=7;
	else                                                             PRCLASS=0;
    PR_FR_L=PR_FR_L1;
    PR_TO_L=PR_TO_L1;
    PR_FR_R=PR_FR_R1;
    PR_TO_R=PR_TO_R1;
	run;
%end; 

/* NS delivered in FGDB - Export FGDB Data to DBF - 01April2014 ->same 2017...*/
/* NS addressing not by roadsegid, but by NID - requires distinct on NID. Some duplication on road name table as well */

%else %if &PR=NS %then %do; 

    proc import datafile="&Pathdata.\&delivery.\&ROADSEG" 
        out=wkspc.&PR._ROADSEG dbms=dbf replace;
    proc import datafile="&Pathdata.\&delivery.\&ADDRANGE"
        out=wkspc.&PR._ADDRANGE dbms=dbf replace;
    proc import
        datafile="&Pathdata.\&delivery.\&STRPLANAME"
        out=wkspc.&PR._STRPLANAME dbms=dbf replace;
    run;


	proc sql;
    create table wkspc.&PR._data as
    select  distinct a.nid as PRRDID,
            a.roadclass as PRCLASS1,
            b.l_hnumf as PR_FR_L1,
            b.l_hnuml as PR_TO_L1,
            b.r_hnumf as PR_FR_R1,
            b.r_hnuml as PR_TO_R1,
            c.namebody as PRSTRNME
    from wkspc.&PR._ROADSEG a
    left join wkspc.&PR._ADDRANGE b
        on a.ADRANGENID=b.NID
    left join wkspc.&PR._STRPLANAME c
        on b.L_OFFNANID=c.NID;
    quit;

/*	The below uses ROADSEGID to join - questions about NID vs ROADSEG and total segment counts generated year over year....
	proc sql;
    create table wkspc.&PR._data as
    select  a.roadsegid as PRRDID,
            a.roadclass as PRCLASS,
            b.l_hnumf as PR_FR_L,
            b.l_hnuml as PR_TO_L,
            b.r_hnumf as PR_FR_R,
            b.r_hnuml as PR_TO_R,
            c.namebody as PRSTRNME
    from wkspc.&PR._ROADSEG a
    left join wkspc.&PR._ADDRANGE b
        on a.ADRANGENID=b.NID
    join wkspc.&PR._STRPLANAME c
        on b.L_OFFNANID=c.NID;
    quit;


    proc sql;
    create table wkspc.&PR._data1 as
    select  a.roadsegid as PRRDID,
            a.roadclass as PRCLASS,
            b.l_hnumf as PR_FR_L,
            b.l_hnuml as PR_TO_L,
            b.r_hnumf as PR_FR_R,
            b.r_hnuml as PR_TO_R,
			b.L_OFFNANID
    from wkspc.&PR._ROADSEG a
    left join wkspc.&PR._ADDRANGE b
        on a.ADRANGENID=b.NID;
    quit;

	proc sql;
    create table wkspc.&PR._data2 as
    select  distinct a.*, b.namebody as PRSTRNME
    from wkspc.&PR._data1 a
    right join wkspc.&PR._STRPLANAME b
        on a.L_OFFNANID=b.NID;
    quit;
*/

/*
	data wkspc.&PR._data (keep=PRRDID PRCLASS PR_FR_L PR_TO_L PR_FR_R PR_TO_R PRSTRNME PRCLASS1);
	set wkspc.&PR._data;
	format PR_FR_L PR_TO_L PR_FR_R PR_TO_R 6.;
	if      COMPRESS(LOWCASE(PRCLASS1))='alleyway/lane'       then PRCLASS=8;
	else if COMPRESS(LOWCASE(PRCLASS1))='arterial'            then PRCLASS=3;
	else if COMPRESS(LOWCASE(PRCLASS1))='collector'           then PRCLASS=4;
	else if COMPRESS(LOWCASE(PRCLASS1))='freeway'  			  then PRCLASS=1;
	else if COMPRESS(LOWCASE(PRCLASS1))='expressway/highway'  then PRCLASS=2;
	else if COMPRESS(LOWCASE(PRCLASS1))='local/street'        then PRCLASS=5;
	else if COMPRESS(LOWCASE(PRCLASS1))='ramp'                then PRCLASS=9;
	else if COMPRESS(LOWCASE(PRCLASS1))='resource/recreation' then PRCLASS=10;
	else if COMPRESS(LOWCASE(PRCLASS1))='rapidtransit'        then PRCLASS=11;
	else if COMPRESS(LOWCASE(PRCLASS1))='servicelane'         then PRCLASS=12;
	else if COMPRESS(LOWCASE(PRCLASS1))='winter'              then PRCLASS=13;
	else if COMPRESS(LOWCASE(PRCLASS1))='local/strata'        then PRCLASS=6;
	else if COMPRESS(LOWCASE(PRCLASS1))='local/unknown'       then PRCLASS=7;
	else                                                             PRCLASS=0;
    PR_FR_L=PR_FR_L1;
    PR_TO_L=PR_TO_L1;
    PR_FR_R=PR_FR_R1;
    PR_TO_R=PR_TO_R1;
	run;
*/

	data wkspc.&PR._data (keep=PRRDID PRCLASS PR_FR_L PR_TO_L PR_FR_R PR_TO_R PRSTRNME);
	set wkspc.&PR._data;
	format PR_FR_L PR_TO_L PR_FR_R PR_TO_R 6.;
	PRCLASS=PRCLASS1;       
    PR_FR_L=PR_FR_L1;
    PR_TO_L=PR_TO_L1;
    PR_FR_R=PR_FR_R1;
    PR_TO_R=PR_TO_R1;
	run;




%end; 

/* SK data model no addresses */
%else %if &PR=SK %then %do; 

    proc import datafile="&Pathdata.\&delivery.\&ROADSEG" 
        out=wkspc.&PR._ROADSEG dbms=dbf replace;
    run;


    proc sql;
    create table wkspc.&PR._data as
    select  nid as PRRDID,
            roadclass as PRCLASS,
            /*left_from as PR_FR_L,
            left_to as PR_TO_L,
            rght_from as PR_FR_R,
            rght_to as PR_TO_R,*/
          postedname||rtnumber1 as PRSTRNME /* benifit of the doubt use route number as name (corresponds with Hwy class)*/
    from wkspc.&PR._ROADSEG;
    quit;

	data wkspc.&PR._data;
	set wkspc.&PR._data;
    length PR_FR_L PR_TO_L PR_FR_R PR_TO_R 8; /* force address variable as NULL for metrics script*/
	run;

*%put import already done;
%end; 

/* YT no following NRN naming convensions */
%else %if &PR=YT %then %do; 

    proc import datafile="&Pathdata.\&delivery.\NRN_ROADSEG.dbf" 
        out=wkspc.&PR._ROADSEG dbms=dbf replace;
    proc import datafile="&Pathdata.\&delivery.\NRN_ADDRANGE.dbf"
        out=wkspc.&PR._ADDRANGE dbms=dbf replace;
    proc import
        datafile="&Pathdata.\&delivery.\NRN_STRPLANAME.dbf"
        out=wkspc.&PR._STRPLANAME dbms=dbf replace;
    run;

	proc sql;
    create table wkspc.&PR._data as
    select  a.roadsegid as PRRDID,
            a.roadclass as PRCLASS,
            b.l_hnumf as PR_FR_L,
            b.l_hnuml as PR_TO_L,
            b.r_hnumf as PR_FR_R,
            b.r_hnuml as PR_TO_R,
            c.namebody as PRSTRNME
    from wkspc.&PR._ROADSEG a
    left join wkspc.&PR._ADDRANGE b
        on a.ADRANGENID=b.NID
    left join wkspc.&PR._STRPLANAME c
        on b.L_OFFNANID=c.NID;
    quit;

	*YT data has non-geobase road class of 14 and 16 that correspond to highway / major roads names. Using as 'Highway';
	data wkspc.&PR._data;
	set wkspc.&PR._data;
	if PRCLASS=14  then PRCLASS=2;
	if PRCLASS=16  then PRCLASS=2;
	run;


%end; 

%else %do; *Following NRCAN model;

    proc import datafile="&Pathdata.\&delivery.\ROADSEG.dbf" 
        out=wkspc.&PR._ROADSEG dbms=dbf replace;
    proc import datafile="&Pathdata.\&delivery.\ADDRANGE.dbf"
        out=wkspc.&PR._ADDRANGE dbms=dbf replace;
    proc import
        datafile="&Pathdata.\&delivery.\STRPLANAME.dbf"
        out=wkspc.&PR._STRPLANAME dbms=dbf replace;
    run;

    proc sql;
    create table wkspc.&PR._data as
    select  a.roadsegid as PRRDID,
            a.roadclass as PRCLASS,
            b.l_hnumf as PR_FR_L,
            b.l_hnuml as PR_TO_L,
            b.r_hnumf as PR_FR_R,
            b.r_hnuml as PR_TO_R,
            c.namebody as PRSTRNME
    from wkspc.&PR._ROADSEG a
    left join wkspc.&PR._ADDRANGE b
        on a.ADRANGENID=b.NID
    left join wkspc.&PR._STRPLANAME c
        on b.L_OFFNANID=c.NID;
    quit;
%end;

* =====================================================================================
    Step 2: Call macro to genarate Stats 
 ======================================================================================;
    %gen_pr_metrics;


* =====================================================================================
    Step 3: Output EXL table &PR._OUT 
 ======================================================================================;
*standard export changes worksheet references entirely - need to use libnmae statement to connect to excel workbook;
/*proc export data=wkspc.&PR._metrics 
    outfile="&path.\Reports\External_sources_Stats_for_NRNV2_Agreements_Current.xls" 
	dbms=xls replace;
    sheet="&PR._OUT";
run;*/

libname rep_xlsx xlsx &resultpath;
/*"V:\DataManagement\Prov_Data_Eval_Metrics\Reports\External_sources_Stats_for_NRNV2_Agreements_Current_tst.xls"*/

/*proc datasets library=rep_xls nolist;
	delete &PR._OUT;
run;*/

data rep_xlsx.&PR._OUT;
	set wkspc.&PR._metrics;
run;

libname rep_xlsx clear;

%mend NRNV2_STAT;

*%NRNV2_STAT(delivery=PE\2016\NRN_PE_15_0_Shape_en);
*%NRNV2_STAT(delivery=NS\2017);
*%NRNV2_STAT(delivery=QC\2016);
*%NRNV2_STAT(delivery=ON\2015\ONRN_20150227);
*%NRNV2_STAT(delivery=AB\2017\nrn_ab_16_0_shape_en);
*%NRNV2_STAT(delivery=BC\2015\BCRN_022415);
*%NRNV2_STAT(delivery=YT\NRN_YT20140326);
%NRNV2_STAT(delivery=SK\2019);
*%NRNV2_STAT(delivery=NB\2017);

/*
proc freq data=wkspc.QC_data noprint;
tables  PRSTRNME/out=PRSTRNME list missing;
run;

proc freq data=wkspc.QC_data noprint;
tables  PR_FR_L/out=PR_FR_L list missing;
run;


proc print data=PR_FR_L;
*where substr(UPCASE(PRSTRNME),1,4)="VOIE";
run;

proc print data=PRSTRNME(obs=2000);
where substr(UPCASE(PRSTRNME),1,4)="VOIE";
run;



proc freq data=wkspc.BC_ADDRANGE;
tables  L_hnumf L_hnuml r_hnumf r_hnumL/list missing;
run;
*/


