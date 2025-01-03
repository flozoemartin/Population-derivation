********************************************************************************

* Other indications for antidepressants - more than 12 months before pregnancy, in the 12 months prior to pregnancy and during pregnancy, including those whose indication was first noted during pregnancy

* Author: Flo Martin

* Date: 29/03/2024

********************************************************************************

* Datasets created by this do-file

* - 	$Datadir\indications\all_indics_hes.dta

********************************************************************************

* Start logging

	log using "$Logdir\5_indications\2_identifying indications in hes.txt", replace
	
********************************************************************************

* Identify episodes pertaining to indications for antidepressants recorded in secondary care

	use "$Datadir\formatted_linked_data\hes_diagnosis_epi.dta", clear
	
	replace icd = subinstr(icd, ".", "",.)
	
	merge m:1 icd using "$Codesdir\ad_indications_icd10.dta", keep(3) nogen
	
	save "$Datadir\indications\indications_hes_diagnosis_epi.dta", replace
	
	use "$Datadir\formatted_linked_data\hes_diagnosis_hosp.dta", clear
	
	replace icd = subinstr(icd, ".", "",.)
	gen epistart_num = admidate_num
	
	merge m:1 icd using "$Codesdir\ad_indications_icd10.dta", keep(3) nogen
	
	save "$Datadir\indications\indications_hes_diagnosis_hosp.dta", replace
	
	use "$Datadir\indications\indications_hes_diagnosis_epi.dta", clear
	append using "$Datadir\indications\indications_hes_diagnosis_hosp.dta"
	
	duplicates drop
	
	save "$Datadir\indications\indications_hes.dta", replace
	
	foreach indic in depression anxiety affective dn ed migraine narco pain stress_incont tt_headache {
		
		preserve
			keep if `indic'==1
			count
			save "$Datadir\indications\hes_`indic'.dta", replace
		restore
		
	}
	
* Identify episodes that occurred relative to pregnancy start

	use "$Datadir\derived_data\pregnancy_cohort_final.dta", clear
	bysort patid: gen bign = _N
	summ bign // n = 14 max number of pregnancies
	local maxpreg = r(max)
	sort patid pregstart_num
	drop bign
	
* Creat binary variables for each patient's pregnancies whether they had a code for depression or anxiety in the periods of interest
		
		* Ever
		
	foreach indic in depression anxiety affective dn ed migraine narco pain stress_incont tt_headache {
		forvalues n=1/`maxpreg' {
			
			use "$Datadir\derived_data\pregnancy_cohort_final_`n'.dta", clear
			keep patid pregid pregstart_num 
			
			merge 1:m patid using "$Datadir\indications\hes_`indic'.dta", keep(3) nogen
			
			keep patid pregid pregstart_num epistart_num icd icd10_desc
			
			keep if epistart_num<=pregstart_num-365
			
			if _N>0 {
			
				sort patid epistart_num
				by patid: egen _seq = seq()
				
				gsort + patid - epistart_num
				by patid: egen count_`indic'_ever = seq()
				label variable count_`indic'_ever "Number of `indic' codes ever before the 12 months prior to pregnancy"
				
				sort patid epistart_num
				keep patid pregid icd icd10_desc count_`indic'_ever

				reshape wide icd icd10_desc, i(patid pregid) j(count_`indic'_ever)
				
				gen `indic'_ever=1

				duplicates drop
				
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
		save "$Tempdatadir\ever_prepreg_`indic'_hes.dta", replace
		
		* 12 months pre-pregnancy
		
		forvalues n=1/`maxpreg' {
			
			use "$Datadir\derived_data\pregnancy_cohort_final_`n'.dta", clear
			keep patid pregid pregstart_num 
			
			merge 1:m patid using "$Datadir\indications\hes_`indic'.dta", keep(3) nogen
			
			keep if epistart_num<pregstart_num & epistart_num>=pregstart_num-365
			
			keep patid pregid pregstart_num epistart_num icd icd10_desc
			
			if _N>0 {
			
				sort patid epistart_num
				by patid: egen _seq = seq()
				
				gsort + patid - epistart_num
				by patid: egen count_`indic'_12mo = seq()
				label variable count_`indic'_12mo "Number of `indic' codes in the 12 months prior to pregnancy"
				
				sort patid epistart_num
				keep patid pregid icd icd10_desc count_`indic'_12mo

				reshape wide icd icd10_desc, i(patid pregid) j(count_`indic'_12mo)
				
				gen `indic'_12mo=1

				duplicates drop
				
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
		save "$Tempdatadir\12mo_prepreg_`indic'_hes.dta", replace
		
		* During pregnancy
		
		forvalues n=1/`maxpreg' {
			
			use "$Datadir\derived_data\pregnancy_cohort_final_`n'.dta", clear
			keep patid pregid pregstart_num pregend_num
			
			merge 1:m patid using "$Datadir\indications\hes_`indic'.dta", keep(3) nogen
			
			keep if epistart_num>=pregstart_num & epistart_num<pregend_num
			
			keep patid pregid pregstart_num epistart_num icd icd10_desc
			
			if _N>0 {
			
				sort patid epistart_num
				by patid: egen _seq = seq()
				
				gsort + patid - epistart_num
				by patid: egen count_`indic'_preg = seq()
				label variable count_`indic'_preg "Number of `indic' codes ever during pregnancy"
				
				sort patid epistart_num
				keep patid pregid icd icd10_desc count_`indic'_preg

				reshape wide icd icd10_desc, i(patid pregid) j(count_`indic'_preg)
				
				gen `indic'_preg=1

				duplicates drop
				
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
		save "$Tempdatadir\preg_`indic'_hes.dta", replace
		
		* After pregnancy
		
		forvalues n=1/`maxpreg' {
			
			use "$Datadir\derived_data\pregnancy_cohort_final_`n'.dta", clear
			keep patid pregid pregstart_num pregend_num
			
			merge 1:m patid using "$Datadir\indications\hes_`indic'.dta", keep(3) nogen
			
			gen _dist= pregend_num-epistart_num
			keep if pregend_num<=epistart_num & _dist>-365
			
			if _N>0 {
			
				sort patid epistart_num
				by patid: egen _seq = seq()
				
				gsort + patid - epistart_num
				by patid: egen count_`indic'_postpreg = seq()
				label variable count_`indic'_postpreg "Number of `indic' codes ever before the 12 months after pregnancy"
				
				sort patid epistart_num
				duplicates drop
				
				keep patid pregid icd icd10_desc count_`indic'_postpreg

				reshape wide icd icd10_desc, i(patid pregid) j(count_`indic'_postpreg)
				
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
		save "$Tempdatadir\postpreg_`indic'_hes.dta", replace
		
	* Merge datasets together

		use "$Tempdatadir\ever_prepreg_`indic'_hes.dta", clear
		keep patid pregid `indic'_ever
		rename `indic'_ever `indic'_ever_hes
		merge 1:1 patid pregid using "$Tempdatadir\12mo_prepreg_`indic'_hes.dta", keepusing(`indic'_12mo) nogen
		rename `indic'_12mo `indic'_12mo_hes
		merge 1:1 patid pregid using "$Tempdatadir\preg_`indic'_hes.dta", keepusing(`indic'_preg) nogen
		rename `indic'_preg `indic'_preg_hes
		merge 1:1 patid pregid using "$Tempdatadir\postpreg_`indic'_hes.dta", keepusing(`indic'_postpreg) nogen
		rename `indic'_postpreg `indic'_postpreg_hes
		merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta", keepusing(patid pregid) nogen
		
		foreach x in ever 12mo preg postpreg {
			
			replace `indic'_`x'_hes = 0 if `indic'_`x'_hes==.
			tab `indic'_`x'_hes
			
		}
		
		gen `indic'_hes = 1 if `indic'_ever_hes==1 | `indic'_12mo_hes==1
		replace `indic'_hes = 0 if `indic'_hes!=1
		
		count
		save "$Datadir\indications\all_`indic'_hes.dta", replace
	
	}
	
	use "$Datadir\indications\all_affective_hes.dta", clear
	
	foreach indic in dn ed migraine narco pain stress_incont tt_headache {
		
		merge 1:1 patid pregid using "$Datadir\indications\all_`indic'_hes.dta", nogen
		
	}
	
	save "$Datadir\indications\all_other_indics_hes.dta", replace
	
/* Use the datasets with all the retained codes to investigate the most commonly recorded code
		
	* Most common codes in each period
	
		* Investigating the frequency of code recording in the 12 months prior to pregnancy
		
	foreach indic in depression anxiety pain {
		
		use "$Tempdatadir\12mo_prepreg_`indic'.dta", clear
	
		reshape long icd icd10_desc, i(patid pregid) j(count)
		drop if icd==""
		
		tab count
		tab icd10_desc
		
		bysort patid (pregid): egen total=max(count)
		drop count
		duplicates drop
		
		bysort icd10_desc (patid): gen n=_N
		bysort n icd10_desc (patid): gen tag=(_n==1)
		replace tag = sum(tag)
		sum tag , meanonly
		gen top10codes = (tag>=(`r(max)'-9))
		sum n if tag==(`r(max)'-9), meanonly
		replace top10codes = 1 if n==`r(max)'

		gen top10codes_desc = icd10_desc if top10codes==1
		replace top10codes_desc = "Other `indic'-related codes" if top10codes==0
		
		tab top10codes_desc
		
		save "$Datadir\indications\12mo_prepreg_`indic'_hes.dta", replace
		
		* Investigating the frequency of code recording during pregnancy
		
		use "$Tempdatadir\preg_`indic'.dta", clear
		
		reshape long icd icd10_desc, i(patid pregid) j(count)
		drop if icd==""
		
		tab count
		tab icd10_desc
		
		bysort patid (pregid): egen total=max(count)
		drop count
		duplicates drop
		
		bysort icd10_desc (patid): gen n=_N
		bysort n icd10_desc (patid): gen tag=(_n==1)
		replace tag = sum(tag)
		sum tag , meanonly
		gen top10codes = (tag>=(`r(max)'-9))
		sum n if tag==(`r(max)'-9), meanonly
		replace top10codes = 1 if n==`r(max)'

		gen top10codes_desc = icd10_desc if top10codes==1
		replace top10codes_desc = "Other `indic'-related codes" if top10codes==0
		
		tab top10codes_desc
		
		save "$Datadir\indications\preg_`indic'_hes.dta", replace
		
		* Investigating the frequency of code recording in the 12 months after pregnancy
		
		use "$Tempdatadir\postpreg_`indic'.dta", clear
		
		reshape long icd icd10_desc, i(patid pregid) j(count)
		drop if icd==""
		
		tab count
		tab icd10_desc
		
		bysort patid (pregid): egen total=max(count)
		drop count
		duplicates drop
		
		bysort icd10_desc (patid): gen n=_N
		bysort n icd10_desc (patid): gen tag=(_n==1)
		replace tag = sum(tag)
		sum tag , meanonly
		gen top10codes = (tag>=(`r(max)'-9))
		sum n if tag==(`r(max)'-9), meanonly
		replace top10codes = 1 if n==`r(max)'

		gen top10codes_desc = icd10_desc if top10codes==1
		replace top10codes_desc = "Other `indic'-related codes" if top10codes==0
		
		tab top10codes_desc
		
		save "$Datadir\indications\postpreg_`indic'_hes.dta", replace
	
	}

********************************************************************************

* Stop logging

  log close

********************************************************************************
