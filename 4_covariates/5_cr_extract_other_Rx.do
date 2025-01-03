********************************************************************************

* Identifying other prescriptions (teratogens, anti-emetics, multivitamins and folic acid) in the 365 days before pregnancy (change to anytime during pregnancy?)

* Author: Harriet Forbes adapted by Flo Martin

* Date: 16/09/2022 

********************************************************************************

* Datasets created

* Prescriptions written 365 days before pregnancy
*	- $Datadir\covariates\multivit_prepreg_Rx.dta
*	- $Datadir\covariates\folic_prepreg_Rx.dta
*	- $Datadir\covariates\antiemetic_prepreg_Rx.dta

* Prescriptions written during pregnancy
*	- $Datadir\covariates\multivit_preg_Rx.dta
*	- $Datadir\covariates\folic_preg_Rx.dta
*	- $Datadir\covariates\antiemetic_preg_Rx.dta

********************************************************************************

* Tell Stata not to pause or display the -more- message	as a default

	set more off, permanently
	
********************************************************************************

* First load in the multivitamin codelist from PREPArE, checked by Dheeraj, and save as a file name compatible with the later code
	
	use "$Codesdir\Prescription_multivitamin_signed_off_DR.dta", clear
	
	gen multivit=1
	keep prodcode multivit
	gen strength=.
	
	save "$Codesdir\Prescription_multivit_signed_off.dta", replace
	
********************************************************************************

* Next, load in the folic acid codelist from PREPArE, checked by Dheeraj, and save as a file name compatible with the later code

	use "$Codesdir\Prescription_folic_acid_signed_off_DR.dta", clear
	
	gen folic=1
		
	tab strength
	gen five_mg = 1 if regexm(strength, "5mg") & substance=="Folic acid"
	replace five_mg = 0 if five_mg==.
	
	keep prodcode folic five_mg
	rename five_mg strength
	
	save "$Codesdir\Prescription_folic_signed_off.dta", replace
	
********************************************************************************

* Next, load in the anti-emetics codelist from PREPArE, checked by Dheeraj, and save as a file name compatible with the later code

	use "$Codesdir\Prescription_Antiemetics_signed_off_DR.dta", clear
	
	gen antiemetic=1
	gen strength=.
	keep prodcode antiemetic strength
	
	save "$Codesdir\Prescription_antiemetic_signed_off.dta", replace

********************************************************************************
	
	use "$Datadir\derived_data\pregnancy_cohort_final.dta", clear
	bysort patid: gen bign = _N
	summ bign // n = 14 max number of pregnancies
	local maxpreg = r(max)
	sort patid pregstart_num
	drop bign
	
* Prescription in 365 days pre-pregnancy

	* All teratogens (minus mood stabilisers), multivitamins, folic acid and anti-emetics prescribed in the 365 days prior to pregnancy
	
	foreach x in multivit folic antiemetic {
		
		use "$Datadir\formatted_cprd_data\All_Therapy_reduced.dta", clear
		keep patid prodcode eventdate_num
		
		* Merge with prodcode
		merge m:1 prodcode using "$Codesdir\Prescription_`x'_signed_off.dta", keep(3) nogen
		sort patid

		save "$Tempdatadir\Rx_`x'_all.dta", replace
	
	}
	
	* In 365 days pre-preg 
	
	foreach x in multivit folic antiemetic {
		forvalues n=1/`maxpreg' {
			
			use "$Datadir\derived_data\pregnancy_cohort_final_`n'.dta", clear
			keep patid pregid pregstart_num 	
			
			*Merge with pregnancy register
			merge 1:m patid using "$Tempdatadir\Rx_`x'_all.dta", keep(3) nogen
			sort patid
			
			gen _dist= pregstart_num-eventdate_num
			keep if _dist<=0 & _dist>-365
			
			gen `x'_prepreg=1
			
			keep patid pregid `x'_prepreg strength
			save "$Tempdatadir\prepreg_`x'_`n'.dta", replace
		
		}
	}
	
		* During pregnancy 
	
	foreach x in multivit folic antiemetic {
		forvalues n=1/`maxpreg' {
			
			use "$Datadir\derived_data\pregnancy_cohort_final_`n'.dta", clear
			keep patid pregid pregstart_num pregend_num	
			
			*Merge with pregnancy register
			merge 1:m patid using "$Tempdatadir\Rx_`x'_all.dta"
			
			*Keep if matched
			keep if _merge==3
			drop _merge
			sort patid
			
			keep if eventdate_num>=pregstart_num & eventdate_num<pregend_num
			
			gen `x'_preg=1
			
			keep patid pregid `x'_preg strength
			save "$Tempdatadir\preg_`x'_`n'.dta", replace
		
		}
	}
	
	* Multivitamins pre-pregnancy
	
	use "$Tempdatadir\prepreg_multivit_1.dta", clear
	forvalues n=2/`maxpreg' {
		
		append using "$Tempdatadir\prepreg_multivit_`n'.dta"
	
	}
	
	duplicates drop
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", nogen keep(2 3)
	replace multivit_prepreg = 0 if multivit_prepreg ==.
	label variable multivit_prepreg "Prescribed multivitamin use in the year before pregnancy (binary)"
	save "$Datadir\covariates\multivit_prepreg_Rx.dta", replace
	
	* Multivitamins during pregnancy
	
	use "$Tempdatadir\preg_multivit_1.dta", clear
	forvalues n=2/`maxpreg' {
		
		append using "$Tempdatadir\preg_multivit_`n'.dta"
	
	}
	
	duplicates drop
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", nogen keep(2 3)
	replace multivit_preg = 0 if multivit_preg ==.
	label variable multivit_preg "Prescribed multivitamin use during pregnancy (binary)"
	save "$Datadir\covariates\multivit_preg_Rx.dta", replace
	
	* Folic acid pre-pregnancy
	
	use "$Tempdatadir\prepreg_folic_1", clear
	forvalues n=2/`maxpreg' {
	
		append using "$Tempdatadir\prepreg_folic_`n'"
	
	}
	
	duplicates drop
	
	reshape wide folic_prepreg, i(patid pregid) j(strength)
	
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", nogen keep(2 3)
	replace folic_prepreg0 = 0 if folic_prepreg0 ==.
	replace folic_prepreg1 = 0 if folic_prepreg1 ==.
	label variable folic_prepreg0 "Prescribed folic acid use (not 5mg) in the year before pregnancy (binary)"
	label variable folic_prepreg1 "Prescribed folic acid use (5mg) in the year before pregnancy (binary)"
	save "$Datadir\covariates\folic_prepreg_Rx.dta", replace
	
	* Folic acid during pregnancy
	
	use "$Tempdatadir\preg_folic_1", clear
	forvalues n=2/`maxpreg' {
	
		append using "$Tempdatadir\preg_folic_`n'"
	
	}
	
	duplicates drop
	
	reshape wide folic_preg, i(patid pregid) j(strength)
	
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", nogen keep(2 3)
	replace folic_preg0 = 0 if folic_preg0 ==.
	replace folic_preg1 = 0 if folic_preg1 ==.
	label variable folic_preg0 "Prescribed folic acid use (not 5mg) during pregnancy (binary)"
	label variable folic_preg1 "Prescribed folic acid use (5mg) during pregnancy (binary)"
	save "$Datadir\covariates\folic_preg_Rx.dta", replace
	
	* Anti-emetics pre-pregnancy
	
	use "$Tempdatadir\prepreg_antiemetic_1.dta", clear
	forvalues n=2/`maxpreg' {
		
		append using "$Tempdatadir\prepreg_antiemetic_`n'.dta"
	
	}
	
	duplicates drop
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", nogen keep(2 3)
	replace antiemetic_prepreg = 0 if antiemetic_prepreg ==.
	label variable antiemetic_prepreg "Anti-emetic medication use in the year before pregnancy (binary)"
	save "$Datadir\covariates\antiemetic_prepreg_Rx.dta", replace
	
	* Anti-emetics during pregnancy
	
	use "$Tempdatadir\preg_antiemetic_1.dta", clear
	forvalues n=2/`maxpreg' {
		
		append using "$Tempdatadir\preg_antiemetic_`n'.dta"
	
	}
	
	duplicates drop
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", nogen keep(2 3)
	replace antiemetic_preg = 0 if antiemetic_preg ==.
	label variable antiemetic_preg "Anti-emetic medication use during pregnancy (binary)"
	save "$Datadir\covariates\antiemetic_preg_Rx.dta", replace

********************************************************************************

* Erase datasets no longer needed

	foreach x in multivit folic antiemetic {
		
		erase "$Tempdatadir\Rx_`x'_all.dta"
		
	}
	
	foreach x in multivit folic antiemetic {
		forvalues n=1/`maxpreg' {
			
			erase "$Tempdatadir\prepreg_`x'_`n'.dta"
			erase "$Tempdatadir\preg_`x'_`n'.dta"
			
		}
	}

********************************************************************************
