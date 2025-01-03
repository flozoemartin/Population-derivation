********************************************************************************

* Deriving alcohol, smoking and BMI

* Author: Flo Martin (adapted from scripts by Paul Madley-Dowd)

* Date: 17/10/2022

********************************************************************************

	use "$Datadir\derived_data\pregnancy_cohort_final.dta", clear
	bysort patid: gen bign = _N
	summ bign // n = 16 max number of pregnancies
	local maxpreg = r(max)
	sort patid pregstart_num
	drop bign

* Smoking

	/* During or pre-pregnancy
	
	cap program drop pr_getsmok
	forvalues n=1/`maxpreg' {

		use "$Datadir\derived_data\pregnancy_cohort_final_`n'.dta", clear
		keep patid pregstart_num gestdays pregid
		run "$Dodir\4_covariates\3a_pr_getsmok.do"
		noi pr_getsmok, clinicalfile("$Datadir\formatted_cprd_data\All_Clinical.dta") ///
		additionalfile("$Datadir\formatted_cprd_data\All_Additional.dta") smokingcodelist("$Codesdir\cr_smokingcodes_reduced.dta") ///
		smokingstatusvar(smokstatus) index(pregstart_num)
		
		* Recode smoking status
		recode smokstatus 12=1
		rename smokstatus smokstatus
		save "$Tempdatadir\smoking_`n'_gold.dta", replace

	}	
	
	use "$Tempdatadir\smoking_1_gold.dta", clear
	forvalues n=2/`maxpreg' {

		append using "$Tempdatadir\smoking_`n'_gold.dta"

	}
	
	keep patid pregid pregstart_num smokstatus
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", keep(2 3) nogen
	label variable smokstatus "Smoking status"
	tab smokstatus, nol
	tab smokstatus
	recode smokstatus 0=0 1=2 2=1
	tab smokstatus
	tab smokstatus, nol
	label define lb_smokstatus 0 "Non-smoker" 1 "Ex-smoker" 2 "Current smoker"
	label values smokstatus lb_smokstatus
	tab smokstatus, m
	preserve
	keep patid pregid smokstatus
	save "$Datadir\covariates\smoking_final.dta", replace */
	
	* During pregnancy
	
	cap program drop pr_getsmok
	forvalues n=1/`maxpreg' {

		use "$Datadir\derived_data\pregnancy_cohort_final_`n'.dta", clear
		keep patid pregstart_num pregend_num gestdays pregid
		run "$Dodir\4_covariates\3f_pr_getsmokpreg.do"
		noi pr_getsmok, clinicalfile("$Datadir\formatted_cprd_data\All_Clinical.dta") ///
		additionalfile("$Datadir\formatted_cprd_data\All_Additional.dta") smokingcodelist("$Codesdir\cr_smokingcodes_reduced.dta") ///
		smokingstatusvar(smokstatus) index(pregstart_num)
		
		* Recode smoking status
		recode smokstatus 12=1
		rename smokstatus smokstatus
		save "$Tempdatadir\smoking_`n'_gold.dta", replace

	}	
	
	use "$Tempdatadir\smoking_1_gold.dta", clear
	forvalues n=2/`maxpreg' {

		append using "$Tempdatadir\smoking_`n'_gold.dta"

	}
	
	keep patid pregid pregstart_num smokstatus
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", keep(2 3) nogen
	label variable smokstatus "Smoking status"
	tab smokstatus, nol
	tab smokstatus
	recode smokstatus 0=0 1=2 2=1
	tab smokstatus
	tab smokstatus, nol
	label define lb_smokstatus 0 "Non-smoker" 1 "Ex-smoker" 2 "Current smoker"
	label values smokstatus lb_smokstatus
	tab smokstatus, m
	rename smokstatus smoke_preg
	preserve
	keep patid pregid smoke_preg
	save "$Datadir\covariates\smoking_preg_final.dta", replace 
	
*******************************************************************************

* Body mass index

* Pre-pregnancy

	forvalues n=1/`maxpreg' {
	
		disp "n = `n'"
		use "$Datadir\derived_data\pregnancy_cohort_final_`n'.dta", clear
		keep patid pregid pregstart_num secondtrim_num pregend_num
		run "$Dodir\4_covariates\3b_pr_getbmistatus.do"
		run "$Dodir\4_covariates\3c_pr_getallbmirecords.do"
		run "$Dodir\4_covariates\3d_pr_getheightstatus.do"
		noi pr_getbmistatus, index(pregstart_num) patientfile("$Datadir\formatted_cprd_data\All_Patient.dta") ///
		clinicalfile("$Datadir\formatted_cprd_data\All_Clinical.dta") ///
		additionalfile("$Datadir\formatted_cprd_data\All_Additional.dta")

		egen bmi_cat=cut(bmi), at(0,18.5,25,30,1000) 
		recode bmi_cat 18.5=1
		recode bmi_cat 25=2
		recode bmi_cat 30=3

		label define bmi_cat 0 Underweight 1 "Normal Weight" 2 "Overweight" 3 "Obese"
		lab val bmi_cat bmi_cat
		tab bmi_cat
		save "$Tempdatadir\bmi_`n'.dta", replace

	}

	use "$Tempdatadir\bmi_1.dta", clear

	forvalues n=2/`maxpreg' {
	
		append using "$Tempdatadir\bmi_`n'.dta"

	}

	keep patid pregid bmi bmi_cat
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", nogen keep(2 3)
	label variable bmi_cat "BMI"
	label define lb_bmi 0 "Underweight, <18 kg/m^2" 1 "Normal weight, 18-<25 kg/m^2" 2 "Overweight, 25-<30 kg/m^2" 3 "Obese, >= 35 kg/m^2" 
	label values bmi_cat lb_bmi
	save "$Datadir\covariates\bmi_final.dta", replace

* Check overall distribution

	use "$Datadir\covariates\bmi_final.dta", clear
	merge m:1 patid pregid using "$Datadir\formatted_linked_data\pregnancyregister.dta", keep(match) nogen keepusing(pregstart_num)
	gen pregyear=year(pregstart_num)
	keep if pregyear>=2010 & pregyear<=2016
	merge m:1 patid using "$Datadir\formatted_linked_data\linkage_eligibility.dta", keep(match) nogen keepusing(hes_apc_e)
	keep if hes_apc==1 
	bysort patid: keep if _n==1
	merge 1:m patid pregstart_num using "$Datadir\derived_data\pregnancy_cohort_final.dta", keep(match) nogen keepusing(updated_outcome)
	tab updated_outcome
	keep if updated_outcome==1 /*keep live births*/
	tab bmi_cat , miss
	count if bmi_cat>=35 & pregyear>=2010 & pregyear<=2016 

********************************************************************************

* Alcohol

	* Ever 

	forvalues n=1/`maxpreg' {

		use "$Datadir\derived_data\pregnancy_cohort_final_`n'.dta", clear
		keep patid pregid pregstart_num gestdays
		run "$Dodir\4_covariates\3e_pr_getalcoholstatus.do"
		noi pr_getalcoholstatus,  clinicalfile("$Datadir\formatted_cprd_data\All_Clinical.dta") ///
		additionalfile("$Datadir\formatted_cprd_data\All_Additional.dta") ///
		alcoholcodelist("$Codesdir\cr_alcoholcodes.dta") alcoholstatusvar(alcstatus) ///
		alcohollevelvar(alclevel) index(pregstart_num)
		save "$Tempdatadir\alc_`n'.dta", replace
		
	}

	use "$Tempdatadir\alc_1.dta", clear
	forvalues n=2/`maxpreg' {

		append using "$Tempdatadir\alc_`n'.dta"

	}

	duplicates drop
	
	gen hazardous_drinking=1 if alclevel==3
	replace hazardous_drinking=0 if alclevel!=3
	keep patid pregid alclevel alcstatus hazardous_drinking
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", keep(2 3) nogen
	label variable hazardous_drinking "Evidence of alcohol problems (binary)"
	label define lb_hazardous_drinking 1 "Yes" 0 "No"
	label values hazardous_drinking lb_hazardous_drinking
	save "$Datadir\covariates\alc_final.dta", replace

* Check overall distribution

	use "$Datadir\covariates\alc_final.dta", clear
	merge m:1 patid pregid using "$Datadir\formatted_linked_data\pregnancyregister.dta", keep(match) nogen keepusing(pregstart_num)
	gen pregyear=year(pregstart_num)
	keep if pregyear>=2010 & pregyear<=2016
	merge m:1 patid using "$Datadir\formatted_linked_data\linkage_eligibility.dta", keep(match) nogen keepusing(hes_apc_e)
	keep if hes_apc_e==1 
	bysort patid: keep if _n==1
	merge 1:m patid pregstart_num using "$Datadir\derived_data\pregnancy_cohort_final.dta", keep(match) nogen keepusing(outcome)
	tab outcome
	keep if outcome==1 /*keep live births*/
	tab hazardous_drinking , miss 

********************************************************************************

* Delete unnecessary datasets
	
	forvalues n=1/`maxpreg' {

		capture erase "$Tempdatadir\smoking_`n'_gold.dta"
		capture erase "$Tempdatadir\alc_`n'.dta"
		capture erase "$Tempdatadir\bmi_`n'.dta"

	}

********************************************************************************
