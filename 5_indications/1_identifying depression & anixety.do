********************************************************************************

* Indications for antidepressants (depression and anxiety) - more than 12 months before pregnancy, in the 12 months prior to pregnancy and during pregnancy, including those whose depression was first noted during pregnancy

* Author: Flo Martin

* Date: 10/01/2023

********************************************************************************

* Datasets created by this do-file

* - 	$Datadir\indications\all_depression.dta
* - 	$Datadir\indications\all_anxiety.dta

********************************************************************************

* Start logging

	log using "$Logdir\5_indications\1_identifying depression & anxiety.txt", replace
	
********************************************************************************
	
* Lift all events of depression from the Clinical and Referral Files

	foreach indic in depression anxiety mood ed dn incont pain migraine headache narcolepsy {
		forvalues x=0/11 {
			
			use "$Datadir\formatted_cprd_data\Clinical_`x'.dta", clear
				
			merge m:1 medcode using "$Codesdir\read_`indic'_codelist.dta", keep(3) nogen
			gen `indic' = 1 
				
			sort patid
				
			save "$Tempdatadir\read_`indic'_Clin_`x'.dta", replace
			
		}
		
		use "$Datadir\formatted_cprd_data\All_Referral.dta", clear
				
		merge m:1 medcode using "$Codesdir\read_`indic'_codelist.dta", keep(3) nogen
		gen `indic' = 1 
				
		sort patid
				
		save "$Tempdatadir\read_`indic'_Ref.dta", replace
		
		use "$Tempdatadir\read_`indic'_Ref.dta", clear
		
		forvalues x=0/11 {
			
			append using "$Tempdatadir\read_`indic'_Clin_`x'.dta"
			
		}
		
		save "$Tempdatadir\all_`indic'.dta", replace
		
	}
	
* Generate a maximum number of pregnancies local macro 
	
	use "$Datadir\derived_data\pregnancy_cohort_final.dta", clear
	bysort patid: gen bign = _N
	summ bign // n = 14 max number of pregnancies
	local maxpreg = r(max)
	sort patid pregstart_num
	drop bign
	
* Creat binary variables for each patient's pregnancies whether they had a code for depression or anxiety in the periods of interest
	
	foreach indic in depression anxiety mood ed dn incont pain migraine headache {
		
		* Ever
	
		forvalues n=1/`maxpreg' {
			
			use "$Datadir\derived_data\pregnancy_cohort_final_`n'.dta", clear
			keep patid pregid pregstart_num 
			
			merge 1:m patid using "$Tempdatadir\all_`indic'.dta", keep(3) nogen
			
			keep if eventdate_num<=pregstart_num-365
			
			if _N>0 {
			
				sort patid eventdate_num
				by patid: egen _seq = seq()
				
				gsort + patid - eventdate_num
				by patid: egen count_`indic'_ever = seq()
				label variable count_`indic'_ever "Number of `indic' codes ever before the 12 months prior to pregnancy"
				
				sort patid eventdate_num
				duplicates drop
				
				keep patid pregid readcode desc count_`indic'_ever

				reshape wide readcode desc, i(patid pregid) j(count_`indic'_ever)
				
				gen `indic'_ever=1
				
			}
			
			else if _N==0 {	
				
				keep patid pregid
				
			}
			
			save "$Tempdatadir\ever_prepreg_`n'.dta", replace
		
		}
		
		use "$Tempdatadir\ever_prepreg_1.dta", clear
		
		forvalues n=2/`maxpreg' {
			
			append using "$Tempdatadir\ever_prepreg_`n'.dta"
			
		}
		
		count
		save "$Tempdatadir\ever_prepreg_`indic'.dta", replace
		
		* 12 months pre-pregnancy
		
		forvalues n=1/`maxpreg' {
			
			use "$Datadir\derived_data\pregnancy_cohort_final_`n'.dta", clear
			keep patid pregid pregstart_num 
			
			merge 1:m patid using "$Tempdatadir\all_`indic'.dta", keep(3) nogen
			
			keep if eventdate_num<pregstart_num & eventdate_num>=pregstart_num-365
			
			if _N>0 {
			
				sort patid eventdate_num
				by patid: egen _seq = seq()
				
				gsort + patid - eventdate_num
				by patid: egen count_`indic'_12mo = seq()
				label variable count_`indic'_12mo "Number of `indic' codes in the 12 months prior to pregnancy"
				
				sort patid eventdate_num
				duplicates drop
				
				keep patid pregid readcode desc count_`indic'_12mo

				reshape wide readcode desc, i(patid pregid) j(count_`indic'_12mo)
				
				gen `indic'_12mo=1
				
			}
			
			else if _N==0 {	
				
				keep patid pregid
				
			}
			
			save "$Tempdatadir\12mo_prepreg_`n'.dta", replace
		
		}
		
		use "$Tempdatadir\12mo_prepreg_1.dta", clear
		
		forvalues n=2/`maxpreg' {
			
			append using "$Tempdatadir\12mo_prepreg_`n'.dta"
			
		}
		
		count
		save "$Tempdatadir\12mo_prepreg_`indic'.dta", replace
		
		* During pregnancy
		
		forvalues n=1/`maxpreg' {
			
			use "$Datadir\derived_data\pregnancy_cohort_final_`n'.dta", clear
			keep patid pregid pregstart_num pregend_num
			
			merge 1:m patid using "$Tempdatadir\all_`indic'.dta", keep(3) nogen
			
			keep if eventdate_num>=pregstart_num & eventdate_num<pregend_num
			
			if _N>0 {
			
				sort patid eventdate_num
				by patid: egen _seq = seq()
				
				gsort + patid - eventdate_num
				by patid: egen count_`indic'_preg = seq()
				label variable count_`indic'_preg "Number of `indic' codes during pregnancy"
				
				sort patid eventdate_num
				duplicates drop
				
				keep patid pregid readcode desc count_`indic'_preg

				reshape wide readcode desc, i(patid pregid) j(count_`indic'_preg)
				
				gen `indic'_preg=1

			}
			
			else if _N==0 {	
				
				keep patid pregid
						
			}
			
			save "$Tempdatadir\preg_`n'.dta", replace
		
		}
		
		use "$Tempdatadir\preg_1.dta", clear
		
		forvalues n=2/`maxpreg' {
			
			append using "$Tempdatadir\preg_`n'.dta"
			
		}
		
		count
		save "$Tempdatadir\preg_`indic'.dta", replace
		
		/* Trimester one
		
		forvalues n=1/`maxpreg' {
			
			use "$Datadir\derived_data\pregnancy_cohort_final_`n'.dta", clear
			keep patid pregid pregstart_num pregend_num
			
			merge 1:m patid using "$Tempdatadir\all_`indic'.dta", keep(3) nogen
			
			keep if eventdate_num>=pregstart_num & eventdate_num<pregend_num
			
			if _N>0 {
			
				gen `indic'_t1=1
				keep patid pregid `indic'_t1
				duplicates drop 
				
			}
			
			else if _N==0 {	
				
				keep patid pregid
						
			}
			
			save "$Tempdatadir\t1_`n'.dta", replace
		
		}
		
		use "$Tempdatadir\t1_1.dta", clear
		
		forvalues n=2/`maxpreg' {
			
			append using "$Tempdatadir\t1_`n'.dta"
			
		}
		
		count
		save "$Tempdatadir\t1_`indic'.dta", replace */
		
		* After pregnancy
		
		forvalues n=1/`maxpreg' {
			
			use "$Datadir\derived_data\pregnancy_cohort_final_`n'.dta", clear
			keep patid pregid pregstart_num pregend_num
			
			merge 1:m patid using "$Tempdatadir\all_`indic'.dta", keep(3) nogen
			
			gen _dist= pregend_num-eventdate_num
			keep if pregend_num<=eventdate_num & _dist>-365
			
			if _N>0 {
			
				sort patid eventdate_num
				by patid: egen _seq = seq()
				
				gsort + patid - eventdate_num
				by patid: egen count_`indic'_postpreg = seq()
				label variable count_`indic'_postpreg "Number of `indic' codes ever before the 12 months after pregnancy"
				
				sort patid eventdate_num
				duplicates drop
				
				keep patid pregid readcode desc count_`indic'_postpreg

				reshape wide readcode desc, i(patid pregid) j(count_`indic'_postpreg)
				
				gen `indic'_postpreg=1
				
			}
			
			else if _N==0 {	
				
				keep patid pregid
						
			}
			
			save "$Tempdatadir\postpreg_`n'.dta", replace
		
		}
		
		use "$Tempdatadir\postpreg_1.dta", clear
		
		forvalues n=2/`maxpreg' {
			
			append using "$Tempdatadir\postpreg_`n'.dta"
			
		}
		
		count
		save "$Tempdatadir\postpreg_`indic'.dta", replace
		
		/* Incident indication during pregnancy
		
		forvalues n=1/`maxpreg' {
			
			use "$Datadir\derived_data\pregnancy_cohort_final_`n'.dta", clear
			keep patid pregid pregstart_num pregend_num
			
			merge 1:m patid using "$Tempdatadir\all_`indic'.dta", keep(3) nogen
			
			bysort patid (eventdate_num): keep if _n==1
			
			keep if eventdate_num>=pregstart_num & eventdate_num<pregend_num
			
			if _N>0 {
			
				gen `indic'_incident_preg=1
				
				keep patid pregid `indic'_incident_preg
				
			}
			
			else if _N==0 {	
				
				keep patid pregid
						
			}
			
			save "$Tempdatadir\incid_preg_`n'.dta", replace
		
		}
		
		use "$Tempdatadir\incid_preg_1.dta", clear
		
		forvalues n=2/`maxpreg' {
			
			append using "$Tempdatadir\incid_preg_`n'.dta"
			
		}
		
		count
		save "$Tempdatadir\incid_preg_`indic'.dta", replace */
		
		use "$Tempdatadir\ever_prepreg_`indic'.dta", clear
		keep patid pregid `indic'_ever
		merge 1:1 patid pregid using "$Tempdatadir\12mo_prepreg_`indic'.dta", keepusing(`indic'_12mo) nogen
		merge 1:1 patid pregid using "$Tempdatadir\preg_`indic'.dta", keepusing(`indic'_preg) nogen
		*merge 1:1 patid pregid using "$Tempdatadir\t1_`indic'.dta", nogen
		merge 1:1 patid pregid using "$Tempdatadir\postpreg_`indic'.dta", keepusing(`indic'_postpreg) nogen
		*merge 1:1 patid pregid using "$Tempdatadir\incid_preg_`indic'.dta", nogen
		merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", keepusing(patid pregid) nogen
		
		foreach x in ever 12mo preg postpreg {
			
			replace `indic'_`x' = 0 if `indic'_`x'==.
			tab `indic'_`x'
			
		}
		
		gen `indic' = 1 if `indic'_ever==1 | `indic'_12mo==1
		replace `indic' = 0 if `indic'!=1
		
		count
		save "$Datadir\indications\all_`indic'_read.dta", replace
		
	}
	
	foreach indic in depression anxiety ed pain {
		
	* Most common codes in each period
	
		* Investigating the frequency of code recording in the 12 months prior to pregnancy
		
		use "$Tempdatadir\12mo_prepreg_`indic'.dta", clear
	
		reshape long readcode desc, i(patid pregid) j(count)
		drop if readcode==""
		
		tab count
		tab desc
		
		bysort desc (patid): gen n=_N
		bysort n desc (patid): gen tag=(_n==1)
		replace tag = sum(tag)
		sum tag , meanonly
		gen top10codes = (tag>=(`r(max)'-9))
		sum n if tag==(`r(max)'-9), meanonly
		replace top10codes = 1 if n==`r(max)'

		gen top10codes_desc = desc if top10codes==1
		replace top10codes_desc = "Other `indic'-related codes" if top10codes==0
		
		tab top10codes_desc
		
		save "$Datadir\indications\12mo_prepreg_`indic'_read.dta", replace
		
		* Investigating the frequency of code recording during pregnancy
		
		use "$Tempdatadir\preg_`indic'.dta", clear
		
		reshape long readcode desc, i(patid pregid) j(count)
		drop if readcode==""
		
		tab count
		tab desc
		
		bysort desc (patid): gen n=_N
		bysort n desc (patid): gen tag=(_n==1)
		replace tag = sum(tag)
		sum tag , meanonly
		gen top10codes = (tag>=(`r(max)'-9))
		sum n if tag==(`r(max)'-9), meanonly
		replace top10codes = 1 if n==`r(max)'

		gen top10codes_desc = desc if top10codes==1
		replace top10codes_desc = "Other `indic'-related codes" if top10codes==0
		
		tab top10codes_desc
		
		save "$Datadir\indications\preg_`indic'_read.dta", replace
		
		* Investigating the frequency of code recording in the 12 months after pregnancy
		
		use "$Tempdatadir\postpreg_`indic'.dta", clear
		
		reshape long readcode desc, i(patid pregid) j(count)
		drop if readcode==""
		
		tab count
		tab desc
		
		bysort desc (patid): gen n=_N
		bysort n desc (patid): gen tag=(_n==1)
		replace tag = sum(tag)
		sum tag , meanonly
		gen top10codes = (tag>=(`r(max)'-9))
		sum n if tag==(`r(max)'-9), meanonly
		replace top10codes = 1 if n==`r(max)'

		gen top10codes_desc = desc if top10codes==1
		replace top10codes_desc = "Other `indic'-related codes" if top10codes==0
		
		tab top10codes_desc
		
		save "$Datadir\indications\postreg_`indic'_read.dta", replace
	
	}
	
********************************************************************************

* Delete unnecessary datasets

	erase "$Tempdatadir\depression_Ref.dta"
	erase "$Tempdatadir\anxiety_Ref.dta"
	
	forvalues x=0/11 {
		
		erase "$Tempdatadir\depression_Clin_`x'.dta"
		erase "$Tempdatadir\anxiety_Clin_`x'.dta"
		
	}
	
	forvalues n=1/`maxpreg' {
			
		erase "$Tempdatadir\ever_prepreg_`n'.dta"
		erase "$Tempdatadir\12mo_prepreg_`n'.dta"
		erase "$Tempdatadir\preg_`n'.dta"
		erase "$Tempdatadir\postpreg_`n'.dta"
		erase "$Tempdatadir\incid_preg_`n'.dta"	
				
	}
	
	foreach indic in depression anxiety {
		
		erase "$Tempdatadir\ever_prepreg_`indic'.dta"
		erase "$Tempdatadir\12mo_prepreg_`indic'.dta"
		erase "$Tempdatadir\preg_`indic'.dta"
		erase "$Tempdatadir\t1_`indic'.dta"
		erase "$Tempdatadir\postpreg_`indic'.dta"
		erase "$Tempdatadir\incid_preg_`indic'.dta"
		
	}
	
*******************************************************************************

* Stop logging

	log close
	
********************************************************************************
