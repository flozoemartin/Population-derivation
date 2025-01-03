********************************************************************************

* This syntax routine pulls together all the pieces of Clinical data provided by CPRD in the new data delivery ready for using with the old Clinical data delivery - All_Clinical as these data are small enough to use as one dataset (unlike Therapy)

* Author: Flo Martin (adapted from scripts by Hein Heuvelman)

* Date: 28/06/2023

********************************************************************************

* Datasets generated by this do-file

	* All Clinical data for using in subsequent PhD projects
	
	* $Datadir\formatted_cprd_data\All_Clinical.dta 

********************************************************************************

* Start logging

	log using "$Logdir\1_formatting\6_formatting clinical.txt", replace
	
********************************************************************************

* Pull together pieces of data within each Patient chunk
	
	foreach y in 0 2 4 7 {
	
		use "$Rawdatadir\redelivery may 23\primary care\Clinical_01_`y'.dta", clear
		
		forvalues x=2/8 {	
			
			append using "$Rawdatadir\redelivery may 23\primary care\Clinical_0`x'_`y'.dta"
			
		}
		
		* Create numerical variables for eventdate and sysdate
		
			gen eventdate_num = date(eventdate, "DMY")
			format eventdate_num %td
			
			gen sysdate_num = date(sysdate, "DMY")
			format sysdate_num %td	
	
		* Label variables
			
			label variable patid "Patient ID"
			label variable eventdate "Date of therapy event - string"
			label variable eventdate_num "Date of therapy event - numerical"
			label variable sysdate "Date entered on Vision - string"
			label variable sysdate_num "Date entered on Vision - numerical"
			label variable consid "Consultation ID: linkage with consultation file when used with pracid"
			label variable constype "Consultation type: category of event"
			label variable medcode "CPRD code for medical term, selected by GP"
			label variable staffid "ID of staff entering data onto Vision, zero ==unknown"
			label variable episode "Episode type for a specific clinical event (lookup EPI)"
			label variable enttype "Identifies representing the structured data area in Vision (lookup Entity)"
			label variable adid "Identified allowing additional info to be retrieved in combination with pracid"
			
		* Sort on patient id and eventdate
		
			sort patid eventdate_num
			
		* Compress
		
			compress *
		
		save "$Datadir\formatted_cprd_data\Clinical_`y'.dta", replace
	
	}
	
	foreach y in 1 3 5 6 10 {
	
		use "$Rawdatadir\redelivery may 23\primary care\Clinical_01_`y'.dta", clear
		
		forvalues x=2/9 {	
			
			append using "$Rawdatadir\redelivery may 23\primary care\Clinical_0`x'_`y'.dta"
			
		}
		
		* Create numerical variables for eventdate and sysdate
		
			gen eventdate_num = date(eventdate, "DMY")
			format eventdate_num %td
			
			gen sysdate_num = date(sysdate, "DMY")
			format sysdate_num %td	
	
		* Label variables
			
			label variable patid "Patient ID"
			label variable eventdate "Date of therapy event - string"
			label variable eventdate_num "Date of therapy event - numerical"
			label variable sysdate "Date entered on Vision - string"
			label variable sysdate_num "Date entered on Vision - numerical"
			label variable consid "Consultation ID: linkage with consultation file when used with pracid"
			label variable constype "Consultation type: category of event"
			label variable medcode "CPRD code for medical term, selected by GP"
			label variable staffid "ID of staff entering data onto Vision, zero ==unknown"
			label variable episode "Episode type for a specific clinical event (lookup EPI)"
			label variable enttype "Identifies representing the structured data area in Vision (lookup Entity)"
			label variable adid "Identified allowing additional info to be retrieved in combination with pracid"
			
		* Sort on patient id and eventdate
		
			sort patid eventdate_num
			
		* Compress
		
			compress *
		
		save "$Datadir\formatted_cprd_data\Clinical_`y'.dta", replace
	
	}
	
	foreach y in 8 9 {
	
		use "$Rawdatadir\redelivery may 23\primary care\Clinical_01_`y'.dta", clear
		
		foreach x in 02 03 04 05 06 07 08 09 10 {	
			
			append using "$Rawdatadir\redelivery may 23\primary care\Clinical_`x'_`y'.dta"
			
		}
		
		* Create numerical variables for eventdate and sysdate
		
			gen eventdate_num = date(eventdate, "DMY")
			format eventdate_num %td
			
			gen sysdate_num = date(sysdate, "DMY")
			format sysdate_num %td	
	
		* Label variables
			
			label variable patid "Patient ID"
			label variable eventdate "Date of therapy event - string"
			label variable eventdate_num "Date of therapy event - numerical"
			label variable sysdate "Date entered on Vision - string"
			label variable sysdate_num "Date entered on Vision - numerical"
			label variable consid "Consultation ID: linkage with consultation file when used with pracid"
			label variable constype "Consultation type: category of event"
			label variable medcode "CPRD code for medical term, selected by GP"
			label variable staffid "ID of staff entering data onto Vision, zero ==unknown"
			label variable episode "Episode type for a specific clinical event (lookup EPI)"
			label variable enttype "Identifies representing the structured data area in Vision (lookup Entity)"
			label variable adid "Identified allowing additional info to be retrieved in combination with pracid"
			
		* Sort on patient id and eventdate
		
			sort patid eventdate_num
			
		* Compress
		
			compress *
		
		save "$Datadir\formatted_cprd_data\Clinical_`y'.dta", replace
	
	}
	
* Old Clinical

	use "$Cohortonedir\formatted_cprd_data\All_Clinical.dta", clear
	
	drop sctid sctdescid sctexpression sctmaptype sctmapversion sctisindicative sctisassured
	sort patid eventdate_num
	
	save "$Datadir\formatted_cprd_data\Clinical_11.dta", replace
	
* Create all clinical

	use "$Datadir\formatted_cprd_data\Clinical_0.dta", clear
	
	forvalues x=1/10 {
		
		append using "$Datadir\formatted_cprd_data\Clinical_`x'.dta"
		
	}
	
	append using "$Datadir\formatted_cprd_data\Clinical_11.dta", gen(sept)
	
	duplicates tag patid adid, gen(dup)
	drop if sept==1 & dup==1
	
	duplicates drop
	count
	
	save "$Datadir\formatted_cprd_data\All_Clinical.dta", replace
	
********************************************************************************

* Stop logging

	log close
	
********************************************************************************
