********************************************************************************

* This syntax routine pulls together all the pieces of Referral data provided by CPRD in the new data delivery ready for using with the old Referral data delivery - All_Referral as these data are small enough to use as one dataset (unlike Therapy)

* Author: Flo Martin (adapted from scripts by Hein Heuvelman)

* Date: 28/06/2023

********************************************************************************

* Datasets generated by this do-file

	* All Referral data for using in subsequent PhD projects
	
	* $Datadir\formatted_cprd_data\All_Referral.dta 

********************************************************************************

* Start logging

	log using "$Logdir\1_formatting\10_formatting referral.txt", replace
	
********************************************************************************

* Pull together pieces of data within each Patient chunk

	use "$Rawdatadir\redelivery may 23\primary care\Referral_01_0.dta", clear
	
	forvalues x=1/10 {	
		
		append using "$Rawdatadir\redelivery may 23\primary care\Referral_01_`x'.dta"
		
	}
	
	append using "$Cohortonedir\formatted_cprd_data\All_Referral.dta"
	count
	
	drop *_num
	
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
		label variable sctid "The mapped SNOMED CT concept ID"
		label variable sctdescid "SNOMED description ID"
		label variable sctexpression "SNOMED expression"
		label variable sctmaptype "SNOMED mapping type"
		label variable sctmapversion "SNOMED mapping version"
		label variable sctisindicative "SCT is indicative"
		label variable sctisassured "SCT is assured"
		label variable staffid "ID of staff entering data onto Vision, zero ==unknown"
		label variable source "Classification of source of referral, e.g. GP, Self"
		label variable nhsspec "Referral speciality according to the NHS classification"
		label variable fhsaspec "Referral speciality according to the Family Health Services Authority classification"
		label variable inpatient "Classification of type of referral, e.g. day case, in-patient"
		label variable attendance "Category describing whether referral event is first visit, follow-up, etc."
		label variable urgency "Classification of the urgency of the referral, e.g. routine, urgent"
				
	* Sort on patient ID and eventdate
			
		sort patid eventdate_num
		
	duplicates drop
	compress
		
	count
	
	save "$Datadir\formatted_cprd_data\All_Referral.dta", replace
	
********************************************************************************

* Stop logging

	log close
	
********************************************************************************
