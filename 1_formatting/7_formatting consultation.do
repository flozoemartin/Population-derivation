********************************************************************************

* This syntax routine pulls together all the pieces of Consultation data provided by CPRD in the new data delivery ready for using with the old Consultation data delivery

* Author: Flo Martin (adapted from scripts by Hein Heuvelman)

* Date: 26/06/2023

********************************************************************************

* Datasets generated by this do-file

	* 12 chunks of Consultation data for using in subsequent PhD projects
	
	* $Tempdatadir\Consultation_0.dta - $Tempdatadir\Consultation_11.dta

********************************************************************************

* Start logging

	log using "$Logdir\1_formatting\7_formatting consultation.txt", replace
	
********************************************************************************
	
* Datasets 0, 2, 4, 5, & 7
	
	foreach y in 0 2 4 5 7 {
	
		use "$Rawdatadir\redelivery may 23\primary care\Consultation_01_`y'.dta", clear
		
		foreach x in 02 03 04 05 06 07 08 {	
			
			append using "$Rawdatadir\redelivery may 23\primary care\Consultation_`x'_`y'.dta"
			
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
		label variable staffid "ID of staff entering data onto Vision, zero ==unknown"
		label variable duration "Length of time between opening and closing consultation record"
			
	* Sort on patient id and eventdate
		
		sort patid eventdate_num
			
	* Compress
		
		compress *
		
		save "$Datadir\formatted_cprd_data\Consultation_`y'.dta", replace
	
	}
	
* Datasets 1,3, & 6

	foreach y in 1 3 6 {
	
		use "$Rawdatadir\redelivery may 23\primary care\Consultation_01_`y'.dta", clear
		
		foreach x in 02 03 04 05 06 07 08 09 {	
			
			append using "$Rawdatadir\redelivery may 23\primary care\Consultation_`x'_`y'.dta"
			
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
		label variable staffid "ID of staff entering data onto Vision, zero ==unknown"
		label variable duration "Length of time between opening and closing consultation record"
			
	* Sort on patient id and eventdate
		
		sort patid eventdate_num
			
	* Compress
		
		compress *
		
		save "$Datadir\formatted_cprd_data\Consultation_`y'.dta", replace
	
	}
	
* Datasets 8, 9 & 10
	
	foreach y in 8 9 10 {
	
		use "$Rawdatadir\redelivery may 23\primary care\Consultation_01_`y'.dta", clear
		
		foreach x in 02 03 04 05 06 07 08 09 10 {	
			
			append using "$Rawdatadir\redelivery may 23\primary care\Consultation_`x'_`y'.dta"
			
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
		label variable staffid "ID of staff entering data onto Vision, zero ==unknown"
		label variable duration "Length of time between opening and closing consultation record"
			
	* Sort on patient id and eventdate
		
		sort patid eventdate_num
			
	* Compress
		
		compress *
		
		save "$Datadir\formatted_cprd_data\Consultation_`y'.dta", replace
	
	}
	
* Old data
	
	use "$Cohortonedir\formatted_cprd_data\All_Consultation.dta", clear
	
	save "$Datadir\formatted_cprd_data\Consultation_11.dta", replace
	
	use "$Datadir\formatted_cprd_data\Consultation_0.dta", clear
	
	forvalues x=1/11 {
		
		append using "$Datadir\formatted_cprd_data\Consultation_`x'.dta"
		
	}
	
	count
	duplicates drop
	
	save "$Datadir\formatted_cprd_data\All_Consultation.dta", replace

********************************************************************************

* Stop logging

	log close

********************************************************************************
