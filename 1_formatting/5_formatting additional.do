********************************************************************************

* This syntax routine pulls together all the pieces of Additional data provided by CPRD in the new data delivery ready for using with the old Additional data delivery

* Author: Flo Martin (adapted from scripts by Hein Heuvelman)

* Date: 26/06/2023

********************************************************************************

* Datasets generated by this do-file

	* 12 chunks of Additional data for using in subsequent PhD projects
	
	* $Tempdatadir\Additional_0.dta - $Tempdatadir\Additional_11.dta

********************************************************************************

* Start logging

	log using "$Logdir\1_formatting\5_formatting additional.txt", replace
	
********************************************************************************
	
* Datasets 0-10
	
	forvalues y=0/10  {
	
		use "$Rawdatadir\redelivery may 23\primary care\Additional_01_`y'.dta", clear	
			
		append using "$Rawdatadir\redelivery may 23\primary care\Additional_02_`y'.dta"
		
		* Label variables
			
		label variable patid "Patient ID"
		label variable enttype "Identifies representing the structured data area in Vision (lookup Entity)"
		label variable adid "Identified allowing additional info to be retrieved in combination with pracid"
		label variable data1 "Depends on enttype (lookup Entity)"

	* Sort on patient id and eventdate
		
		sort patid
		
		tostring data7, replace
		format data7 %12s
		
		save "$Datadir\formatted_cprd_data\Additional_`y'.dta", replace
	
	}
	
	use "$Cohortonedir\formatted_cprd_data\All_Additional.dta", clear
	
	gen sept = 1 
	
	save "$Datadir\formatted_cprd_data\Additional_11.dta", replace
	
	use "$Datadir\formatted_cprd_data\Additional_0.dta", clear
	
	forvalues x=1/11 {
		
		append using "$Datadir\formatted_cprd_data\Additional_`x'.dta"
		
	}
	
	count
	duplicates tag patid adid, gen(dup)
	
	count if dup==1 & sept==1
	drop if dup==1 & sept==1
	
	save "$Datadir\formatted_cprd_data\All_Additional.dta", replace

********************************************************************************
	
* Stop logging

	log close

********************************************************************************
