*******************************************************************************

* Maternal neurodevelopmental conditions

* Author: Harriet Forbes (adapted by Flo Martin)

* Date: 30/09/2022

*******************************************************************************

* Datasets generated by this do-file

*  - $Datadir\covariates\long_term_neurodev_final.dta

*******************************************************************************

	use "$Datadir\derived_data\pregnancy_cohort_final.dta", clear
	bysort patid: gen bign = _N
	summ bign // n = 14 max number of pregnancies
	local maxpreg = r(max)
	sort patid pregstart_num
	drop bign

* Identify bipolar, schizophrenia and other at pregnancy start

	* append code lists

	use "$Codesdir\ReadCode_ASD_signed_off_DR.dta", replace
	gen autism=1
	
	merge 1:1 medcode using "$Codesdir\READ_ADHD_codelist_signed_off_DR.dta"
	gen adhd=1 if _m==3 | _m==2
	drop _m
	
	merge 1:1 medcode using "$Codesdir\ReadCode_ID_signed_off_DR.dta"
	gen id=1 if _m==3 | _m==2
	drop _m
	
	duplicates list medcode readterm // n=0
	
	keep medcode autism adhd id
	save "$Tempdatadir\all_codes_neurodev.dta", replace
	
* Identify ASD, ADHD and ID records at cohort entry

	/* Lift all events relating to ASD from Clinical and referral Files
	
	use "$Datadir\formatted_cprd_data\All_Clinical.dta", clear
	append using "$Datadir\formatted_cprd_data\All_Referral.dta"
	keep patid medcode eventdate_num /*to reduce dataset size*/
	compress
	*drop if medcode<78 - what is this?
	save "$Tempdatadir\clinical_reduced.dta", replace*/
	
	foreach var in autism adhd id {

		use "$Tempdatadir\all_codes_neurodev.dta", clear 
		keep if `var'==1 
		keep medcode `var'
		duplicates drop

		merge 1:m medcode using "$Tempdatadir\clinical_reduced.dta", keep(match) nogen
		rename eventdate `var'_dx_date
		drop if `var'_dx_date==.
		keep patid `var'_dx_date `var'
		bysort patid (`var'_dx_date): keep if _n==1 /*keeps earliest diagnosis: create 1 record per patient*/
		save "$Tempdatadir\codes_`var'_all", replace

	}
	
	foreach var in autism adhd id {
		forvalues x=1/`maxpreg' {

			use "$Tempdatadir\codes_`var'_all", clear
			keep patid `var' `var'_dx_date
			merge 1:1 patid using "$Datadir\derived_data\pregnancy_cohort_final_`x'", keep(match) nogen
			keep patid `var' `var'_dx_date pregend_num pregid
			save "$Tempdatadir\codes_`var'_`x'", replace

		}
	}
	
	foreach var in autism adhd id {
		
		use "$Tempdatadir\codes_`var'_1", clear

		forvalues x=2/`maxpreg' {

			append using "$Tempdatadir\codes_`var'_`x'"
	
		}

		count
		drop if `var'_dx_date>pregend_num /*drop if 1st Dx is after pregnancy end date*/
		keep patid pregid `var' `var'_dx_date

		count
		save "$Tempdatadir\final_`var'", replace

	}
	
	use "$Tempdatadir\final_autism.dta", clear
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", nogen
	merge 1:1 patid pregid using "$Tempdatadir\final_adhd.dta", nogen
	merge 1:1 patid pregid using "$Tempdatadir\final_id.dta", nogen
	keep patid pregid autism adhd id
	replace autism = 0 if autism==.
	replace adhd = 0 if adhd ==. 
	replace id = 0 if id ==. 
	save "$Datadir\covariates\long_term_neurodev_final", replace
	
*******************************************************************************

* Delete unnecessary datasets

	foreach var in autism adhd id {
		forvalues x=1/`maxpreg' {
			
			erase "$Tempdatadir\codes_`var'_`x'.dta"
			
		}
	}
	
	foreach var in autism adhd id {
		
		erase "$Tempdatadir\final_`var'.dta"
		
	}

*******************************************************************************