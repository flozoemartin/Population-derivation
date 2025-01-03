********************************************************************************

* Identifying mental health-related prescriptions (anxiolytics/hypnotics, antipsychotics or anti-seizure medications minus sodium valproate) in the 365 days before pregnancy and during pregnancy

* Author: Harriet Forbes (adapted by Flo Martin)

* Date: 16/09/2022

********************************************************************************

* Datasets created

* Prescriptions written 365 days before pregnancy
*	- $Datadir\covariates\antipsychotics_prepreg_Rx.dta
*	- $Datadir\covariates\benzos_prepreg_Rx.dta
*	- $Datadir\covariates\zdrugs_prepreg_Rx.dta
*	- $Datadir\covariates\moodstabs_prepreg_Rx.dta

* Prescriptions written during pregnancy
*	- $Datadir\covariates\antipsychotics_preg_Rx.dta
*	- $Datadir\covariates\benzos_preg_Rx.dta
*	- $Datadir\covariates\zdrugs_preg_Rx.dta
*	- $Datadir\covariates\moodstabs_preg_Rx.dta

********************************************************************************

* Tell Stata not to pause or display the -more- message	as a default

	set more off, permanently
	
********************************************************************************

* First load in the antipsychotics codelist from PREPArE, checked by Dheeraj, and save as a file name compatible with the later code
	
	use "$Codesdir\Prescription_antipsychotics_signed_off_DR.dta", clear
	
	tab substance
	gen antipsychotics = 1 
	keep prodcode productname antipsychotics
	
	save "$Codesdir\Prescription_antipsychotics_signed_off.dta", replace
	
********************************************************************************

* Benzodiazepines - checked by Victoria and Dheeraj 

	use "$Codesdir\hypnotics_codelist_VNS.dta", clear
	
	keep if benzos==1 
	keep prodcode productname benzos
	
	save "$Codesdir\Prescription_benzos_signed_off.dta", replace	
	
********************************************************************************

* Z-drugs - checked by Victoria and Dheeraj

	use "$Codesdir\hypnotics_codelist_VNS.dta", clear
	
	keep if zdrugs==1
	keep prodcode productname zdrugs
	
	save "$Codesdir\Prescription_zdrugs_signed_off.dta", replace

********************************************************************************

* Mood stabilisers - checked by Dheeraj

		use "$Codesdir\Prescription_AEDs_signed_off_DR.dta", clear
		drop _merge
		
		keep prodcode productname aed aed_class
		order prodcode productname aed aed_class
		rename aed moodstabs
		
		save "$Codesdir\Prescription_moodstabs_signed_off.dta", replace
	
********************************************************************************
	
	use "$Datadir\derived_data\pregnancy_cohort_final.dta", clear
	bysort patid: gen bign = _N
	summ bign // n = 14 max number of pregnancies
	local maxpreg = r(max)
	sort patid pregstart_num
	drop bign
	
* Prescription in 365 days pre-pregnancy & during pregnancy

	* All anxiolytics & hypnotics, antipsychotics and anti-seizure medications (minus valproate which will be included in the teratogens list)
	
	foreach x in antipsychotics benzos zdrugs moodstabs {
		
		use "$Datadir\formatted_cprd_data\All_Therapy_reduced.dta", clear
		
		* Merge with prodcode
		merge m:1 prodcode using "$Codesdir/Prescription_`x'_signed_off.dta", keepusing(prodcode)
		
		* Keep if matched
		keep if _merge==3
		drop _merge
		sort patid

		save "$Tempdatadir\Rx_all_`x'.dta", replace
	
	}
	
	* In 365 days pre-preg 
	
	foreach x in antipsychotics benzos zdrugs moodstabs {
		forvalues n=1/`maxpreg' {
			
			use "$Datadir\derived_data\pregnancy_cohort_final_`n'.dta", clear
			keep patid pregid pregstart_num 	
			
			*Merge with pregnancy register
			merge 1:m patid using "$Tempdatadir\Rx_all_`x'.dta"
			
			*Keep if matched
			keep if _merge==3
			drop _merge
			sort patid
			
			gen _dist= pregstart_num-eventdate_num
			keep if _dist<=0 & _dist>-365
			
			gen `x'_prepreg=1
			
			keep patid pregid `x'_prepreg
			save "$Tempdatadir\prepreg_`x'_`n'.dta", replace
		
		}
	}
	
	* During pregnancy
	
		foreach x in antipsychotics benzos zdrugs moodstabs {
		forvalues n=1/`maxpreg' {
			
			use "$Datadir\derived_data\pregnancy_cohort_final_`n'.dta", clear
			keep patid pregid pregstart_num pregend_num	
			
			*Merge with pregnancy register
			merge 1:m patid using "$Tempdatadir\Rx_all_`x'.dta"
			
			*Keep if matched
			keep if _merge==3
			drop _merge
			sort patid
			
			keep if eventdate_num>=pregstart_num & eventdate_num<pregend_num
			
			gen `x'_preg=1
			
			keep patid pregid `x'_preg
			save "$Tempdatadir\preg_`x'_`n'.dta", replace
		
		}
	}
	
	* Antipsychotics pre-pregnancy
	
	use "$Tempdatadir\prepreg_antipsychotics_1.dta", clear
	forvalues n=2/`maxpreg' {
		
		append using "$Tempdatadir\prepreg_antipsychotics_`n'.dta"
	
	}
	
	duplicates drop
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", nogen keep(2 3)
	replace antipsychotics_prepreg = 0 if antipsychotics_prepreg ==.
	label variable antipsychotics_prepreg "Antipsychotic use in the year before pregnancy (binary)"
	keep patid pregid antipsychotics_prepreg 
	save "$Datadir\covariates\antipsychotics_prepreg_Rx.dta", replace
	
	* Antipsychotics during pregnancy
	
	use "$Tempdatadir\preg_antipsychotics_1.dta", clear
	forvalues n=2/`maxpreg' {
		
		append using "$Tempdatadir\preg_antipsychotics_`n'.dta"
	
	}
	
	duplicates drop
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", nogen keep(2 3)
	replace antipsychotics_preg = 0 if antipsychotics_preg ==.
	label variable antipsychotics_preg "Antipsychotic use during pregnancy (binary)"
	keep patid pregid antipsychotics_preg
	save "$Datadir\covariates\antipsychotics_preg_Rx.dta", replace
	
	* Benzodiazepines pre-pregnancy
	
	use "$Tempdatadir\prepreg_benzos_1", clear
	forvalues n=2/`maxpreg' {
	
		append using "$Tempdatadir\prepreg_benzos_`n'"
	
	}
	
	duplicates drop
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", nogen keep(2 3)
	replace benzos_prepreg = 0 if benzos_prepreg ==.
	label variable benzos_prepreg "Benzodiazepine use in the year before pregnancy (binary)"
	keep patid pregid benzos_prepreg
	save "$Datadir\covariates\benzos_prepreg_Rx.dta", replace
	
	* Benzodiazepines during pregnancy
	
	use "$Tempdatadir\preg_benzos_1", clear
	forvalues n=2/`maxpreg' {
	
		append using "$Tempdatadir\preg_benzos_`n'"
	
	}
	
	duplicates drop
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", nogen keep(2 3)
	replace benzos_preg = 0 if benzos_preg ==.
	label variable benzos_preg "Benzodiazepine use during pregnancy (binary)"
	keep patid pregid benzos_preg
	save "$Datadir\covariates\benzos_preg_Rx.dta", replace
	
	* Z-drugs pre-pregnancy
	
	use "$Tempdatadir\prepreg_zdrugs_1", clear
	forvalues n=2/`maxpreg' {
	
		append using "$Tempdatadir\prepreg_zdrugs_`n'"
	
	}
	
	duplicates drop
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", nogen keep(2 3)
	replace zdrugs_prepreg = 0 if zdrugs_prepreg ==.
	label variable zdrugs_prepreg "Z-drug use in the year before pregnancy (binary)"
	keep patid pregid zdrugs_prepreg
	save "$Datadir\covariates\zdrugs_prepreg_Rx.dta", replace
	
	* Z-drugs during pregnancy
	
	use "$Tempdatadir\preg_zdrugs_1", clear
	forvalues n=2/`maxpreg' {
	
		append using "$Tempdatadir\preg_zdrugs_`n'"
	
	}
	
	duplicates drop
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", nogen keep(2 3)
	replace zdrugs_preg = 0 if zdrugs_preg ==.
	label variable zdrugs_preg "Z-drug use during pregnancy (binary)"
	keep patid pregid zdrugs_preg
	save "$Datadir\covariates\zdrugs_preg_Rx.dta", replace
	
	* Mood stabilisers pre-pregnancy
	
	use "$Tempdatadir\prepreg_moodstabs_1.dta", clear
	forvalues n=2/`maxpreg' {
		
		append using "$Tempdatadir\prepreg_moodstabs_`n'.dta"
	
	}
	
	duplicates drop
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", nogen keep(2 3)
	replace moodstabs_prepreg = 0 if moodstabs_prepreg ==.
	label variable moodstabs "Mood stabilier use in the year before pregnancy (binary)"
	keep patid pregid moodstabs_prepreg
	save "$Datadir\covariates\moodstabs_prepreg_Rx.dta", replace
	
	* Mood stabilisers during pregnancy
	
	use "$Tempdatadir\preg_moodstabs_1.dta", clear
	forvalues n=2/`maxpreg' {
		
		append using "$Tempdatadir\preg_moodstabs_`n'.dta"
	
	}	
	
	duplicates drop
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", nogen keep(2 3)
	replace moodstabs_preg = 0 if moodstabs_preg ==.
	label variable moodstabs "Mood stabilier use during pregnancy (binary)"
	keep patid pregid moodstabs_preg
	save "$Datadir\covariates\moodstabs_preg_Rx.dta", replace
	
	
********************************************************************************

* Erase datasets no longer needed

	foreach x in antipsychotics benzos zdrugs moodstabs {
			
		erase "$Tempdatadir\Rx_all_`x'.dta"
			
	}
	
	foreach x in antipsychotics benzos zdrugs moodstabs {
		forvalues n=1/`maxpreg' {

			erase "$Tempdatadir\prepreg_`x'_`n'.dta"
			erase "$Tempdatadir\preg_`x'_`n'.dta"
			
		}
	}
		
********************************************************************************
