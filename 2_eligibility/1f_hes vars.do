*************************************************************************************

* Deriving birthweight, sex and gestational length (in weeks) from HES Maternity from birth records (for babies in the Mother Baby Link with linked HES data) and delivery records (for mothers in the Pregnancy Register with linked HES data)

* The HES Maternity dataset consists of both birth and delivery records. Birth records contain information on a single baby (affiliated with the baby's patid) whereas delivery records might have information on more than one baby if a multiple birth (because the record is affiliated with mums patid) 

* Linking Data for Mothers and Babies in De-Identified Electronic Health Data (Harron et al.)  
		* DOI https://doi.org/10.1371/journal.pone.0164667

* Author: Flo Martin 

* Date started: 25/02/2023

*************************************************************************************

* Datasets created by this do-file

* 		- $Datadir\derived_data\pregreg_bw_gl.dta
*		  - $Datadir\derived_data\labour_induction.dta
	
*************************************************************************************

* Delivery records (affiliated with MUM's patid)

	* Identifying likely delivery records (for merging on MATERNAL patid) as per Harron et al. supplementary material
		
		use "$Datadir\formatted_linked_data\hes_maternity.dta", clear
		keep patid spno epikey epistart_num epiend_num delprean delposan antedur birordr_num birweit delchang delmeth_num delplac delinten anasdate_num anagest gestat birstat delonset matage postdur biresus sexbaby_num delstat neocare numbaby_num numpreg
		
		duplicates report
		* De-duplicate at this stage of variables to use
		duplicates drop
		
		* Apply exclusion criteria from Harron et al. supplementary
		
		* Exclude those that have all missing variables or only one complete variable
		egen flag = rowmiss(delprean delposan antedur birordr_num birweit delchang delmeth_num delplac delinten anasdate_num anagest gestat birstat delonset matage postdur biresus sexbaby_num delstat)
		
		tab flag
		drop if flag==19 // missing in all variables
		drop if flag==18 // only one complete variable
		
		drop flag delplac
		
		count // 2,138,107 records
		
		* Exclude those with implausible maternal age
		tab matage
		drop if matage>50 // unlikely to be mother or days for infant
		drop if matage<12 // unlikely to be mother
		
		/* these age thresholds are flexible based on your own eligibility criteria */
		
		* Exclude episodes that are unfinished
		drop if epiend_num==.
		
		* Exclude episodes where gestational length is incompatible with a delivery (but compatible with a loss) - again flexible depending on thresholds set within your study
		drop if gestat<22
		
		* Save dataset of likely delivery records in HES Maternity
		save "$Tempdatadir\hes_mat_deliveries.dta", replace
	
* Birth records (affiliated with BABY's patid)

	* Identify birth episodes in the HES Diagnosis dataset for merging with birth records later on (as per Harron et al.)
		
		use "$Datadir\formatted_linked_data\hes_diagnosis_epi.dta", clear
		
		gen icd_stem =""
		replace icd_stem = "Z37" if strmatch(icd, "Z37*")
		replace icd_stem = "Z38" if strmatch(icd, "Z38*")
	
		tab icd icd_stem
		
		keep if icd_stem!=""
		
		keep patid spno epikey epistart_num icd_stem
		duplicates report
		duplicates drop
		
		bysort patid spno epikey epistart_num: egen _presseq=seq()
		
		reshape wide icd_stem, i(patid spno epikey epistart_num) j(_presseq)
		
		save "$Tempdatadir\icd_10_deliveries.dta", replace
	
	* Identifying likely birth records (for merging on BABY patid)
	
	use "$Datadir\formatted_linked_data\hes_maternity.dta", clear
	keep patid spno epikey epistart_num epiend_num delprean delposan antedur birordr_num birweit delchang delmeth_num delinten anasdate_num anagest gestat birstat delonset matage postdur biresus sexbaby_num delstat neocare numbaby_num numpreg
	
	* Flag if information regarding neonatal care is available
	gen flag = 1 if neocare==0 | neocare==1 | neocare==2 | neocare==3
	
	* Create a reshape variable based on episodes among individual patients (maximum 9 rows per episode)
	bysort patid spno epikey epistart_num: egen _presseq=seq()
	
	* Reshape the data so that each episode is represented by 9 columns per variable in HES Maternity
	reshape wide delprean delposan antedur birordr_num birweit delchang delmeth_num delinten anasdate_num anagest gestat birstat delonset matage postdur biresus sexbaby_num delstat neocare numbaby_num numpreg, i(patid spno epikey epistart_num epiend_num flag) j(_presseq)
	
	* Drop "duplicate" variables of those we are not interested in - for individual studies where these are of interest, all 9 can be retained by removing them from this loop
	foreach var in delinten delonset delprean delposan numbaby_num antedur delchang anagest matage numpreg postdur neocare anasdate_num {
			
			gen `var' = `var'1
			drop `var'1 `var'2 `var'3 `var'4 `var'5 `var'6 `var'7 `var'8 `var'9
			tab `var'
			
		}
		
	format %td anasdate_num
	
	duplicates report patid spno epikey epistart_num
		
	* Now the dataset should have all 9 instances of birthweight, sex and gestat that were recorded across each episode (most of which will be missing or duplicates) for checking and retaining later on as one variable
	
	* Also, patid spno epikey epistart_num uniquely identify episodes for merging with HES Episodes and ICD-10 deliveries dataset for applying the rest of our exclusion criteria (guided by Harron et al. supplementary material)
	
	merge 1:1 patid spno epikey epistart_num using "$Datadir\formatted_linked_data\hes_episodes.dta", keep(3) nogen
	
	merge 1:1 patid spno epikey using "$Tempdatadir\icd_10_deliveries.dta", keep(2 3) nogen
	
	* epitype denotes type of episode - (1) general episode, (2) delivery episode and (3) birth episode
	tab epitype
	br if epitype==1
	
	* Flag those with evidence of a delivery
	replace flag = 1 if admimeth=="82" | admimeth=="83"
	
	tab epitype if flag==1
	
	* Apply exclusion criteria - drop those that aren't flagged and aren't a birth episode
	drop if epitype!=3 & flag!=1 // all potential birth episodes
	
	save "$Tempdatadir\birth_episodes.dta", replace
	
*************************************************************************************

* Matching birth records for singleton babies in the Mother Baby Link with linked HES data

	* Merging baby patid's in the Mother Baby Link with likely birth episodes from HES Maternity and retaining birweit, gestat and sexbaby
	
	use "$Datadir\derived_data\pregnancy_cohort_final.dta", clear
	drop if babypatid==.
	count
	tab updated_outcome
	tab mblbabies
	drop if mblbabies>1 | multiple_ev==1
	
	rename patid mumpatid
	merge 1:1 mumpatid babypatid using "$Datadir\formatted_linked_data\mbl_baby.dta", keep(3) nogen
	keep mumpatid babypatid pregid pregend_num
	
	* n = 580,349 singleton babies in the Mother Baby Link 
	
	* Renaming babypatid patid because we want to merge to the birth records that we identified in HES Maternity (therefore affiliated with baby's patid)
	rename babypatid patid
	
	merge 1:m patid using "$Tempdatadir\birth_episodes.dta", keep(3) nogen
	
	count 			// n = 233,816 birth records...
	codebook patid	// ...among 232,003 babies
	
	sort patid epistart_num
	
	* Create a reshape variable for babies with multiple birth records
	bysort patid: egen _presseq=seq()
	tab _presseq
	* n = 1,759 with 2 birth records and n = 51 with 3 birth records and n = 3 with 4
	
	* Birthweight
	preserve
	
		keep patid pregid mumpatid birweit* _presseq
		
		* Reshape to achieve one record per patient but retaining every possible birthweight that has been recorded per patient
		reshape wide birweit1 birweit2 birweit3 birweit4 birweit5 birweit6 birweit7 birweit8 birweit9, i(patid pregid mumpatid) j(_presseq)
		
		count			// n = 232,003 birth records...
		codebook patid	// ...among 232,003 babies
		
		gen birweit = max(birweit11, birweit21, birweit31, birweit41, birweit51, birweit61, birweit71, birweit81, birweit91, birweit12, birweit22, birweit32, birweit42, birweit52, birweit62, birweit72, birweit82, birweit92, birweit13, birweit23, birweit33, birweit43, birweit53, birweit63, birweit73, birweit83, birweit93, birweit14, birweit24, birweit34, birweit44, birweit54, birweit64, birweit74, birweit84, birweit94)
		
		keep patid pregid mumpatid birweit
		
		sum birweit // n = 166,894 with a birthweight somewhere in the birth record
		
		gen birth_record=1
		
		rename patid babypatid
		rename mumpatid patid
		
		save "$Tempdatadir\birweit_mbl_singletons.dta", replace
	
	restore
	
	* Gestational length in weeks
	preserve
	
		keep patid pregid mumpatid gestat* _presseq
		
		reshape wide gestat1 gestat2 gestat3 gestat4 gestat5 gestat6 gestat7 gestat8 gestat9, i(patid pregid mumpatid) j(_presseq)
		
		gen gestat = max(gestat11, gestat21, gestat31, gestat41, gestat51, gestat61, gestat71, gestat81, gestat91, gestat12, gestat22, gestat32, gestat42, gestat52, gestat62, gestat72, gestat82, gestat92, gestat13, gestat23, gestat33, gestat43, gestat53, gestat63, gestat73, gestat83, gestat93, gestat14, gestat24, gestat34, gestat44, gestat54, gestat64, gestat74, gestat84, gestat94)
		
		sum gestat
		
		keep patid pregid mumpatid gestat 
		
		gen birth_record=1
		
		rename patid babypatid
		rename mumpatid patid
		
		save "$Tempdatadir\gestat_mbl_singletons.dta", replace
	
	restore
	
	* Sex of the baby
	keep patid pregid mumpatid sexbaby_num* _presseq
	
	reshape wide sexbaby_num1 sexbaby_num2 sexbaby_num3 sexbaby_num4 sexbaby_num5 sexbaby_num6 sexbaby_num7 sexbaby_num8 sexbaby_num9, i(patid pregid mumpatid) j(_presseq)
	
	gen sex = max(sexbaby_num11, sexbaby_num21, sexbaby_num31, sexbaby_num41, sexbaby_num51, sexbaby_num61, sexbaby_num71, sexbaby_num81, sexbaby_num91, sexbaby_num12, sexbaby_num22, sexbaby_num32, sexbaby_num42, sexbaby_num52, sexbaby_num62, sexbaby_num72, sexbaby_num82, sexbaby_num92, sexbaby_num13, sexbaby_num23, sexbaby_num33, sexbaby_num43, sexbaby_num53, sexbaby_num63, sexbaby_num73, sexbaby_num83, sexbaby_num93, sexbaby_num14, sexbaby_num24, sexbaby_num34, sexbaby_num44, sexbaby_num54, sexbaby_num64, sexbaby_num74, sexbaby_num84, sexbaby_num94)
	
	tab sex
	
	keep patid pregid mumpatid sex
	
	gen birth_record=1
	
	rename patid babypatid
	rename mumpatid patid
	
	save "$Tempdatadir\sex_mbl_singletons.dta", replace
	
*************************************************************************************

* Matching delviery records for singleton deliveries in the Pregnancy Register with linked HES data
	
* Identifying the remainder of the deliveries that do not have a baby birth record in HES and finding their birweit, gestat and sexbaby
	
	use "$Datadir\derived_data\pregnancy_cohort_final.dta", clear
	count
	
	* Keep deliveries in the Pregnancy Register
	tab updated_outcome
	keep if inlist(updated_outcome, 1, 2, 3, 11, 12)
	tab updated_outcome
	
	* Drop multiple pregnancies
	drop if multiple_ev==1
	keep if mblbabies==1 | mblbabies==.
	
	keep patid pregid pregend_num
	sort patid pregend_num
	
	* Create a reshape variable to reshape data (so each row represents a patient) which can then we merged with the HES Maternity delivery records and looped over each pregnancy to see which delivery records match the pregnancy dates and are likely to pertain to each pregnancy
	bysort patid: egen _presseq=seq()
	tab _presseq
	
	reshape wide pregid pregend_num, i(patid) j(_presseq) 

	merge 1:m patid using "$Tempdatadir\hes_mat_deliveries.dta", keep(3) nogen
	
	save "$Tempdatadir\delivery_episodes.dta", replace
	
	foreach var in gestat sexbaby_num birweit { // variables you want to pull out of HES
		forvalues x=1/11 { // based on reshape variable value for your dataset
	
			use "$Tempdatadir\delivery_episodes.dta", clear
			
			recode sexbaby_num 3=.
			gen multi_flag =. 		// want to identify potential multiple births where more than one conflicting birthweight is recorded in the HES record  - evidence of more than one baby delivered
			
			gen diff = pregend_num`x' - epistart_num
			summ diff
			
			keep if diff < 175 & diff >-175 // these episodes likely to pertain to pregnancy of interest
			
			if _N>0 { // if the dataset isn't empty run the following
			
				keep patid pregid`x' pregend_num`x' epistart_num `var' multi_flag
				
				count
				codebook patid
				
				bysort patid epistart_num: egen _presseq=seq() // create a reshape variable per episode in each patient (maximum of 9 rows for each episode - refers to tails variable in HES Maternity)
				
				tab _presseq
				gsort -_presseq
				
				if _presseq==8 { 
				
					disp "_presseq==8"
					reshape wide `var', i(pregid`x' pregend_num`x' epistart_num multi_flag) j(_presseq)
					gen `var' = max(`var'1, `var'2, `var'3, `var'4, `var'5, `var'6,`var'7, `var'8)
					replace multi_flag = 1 if `var'1!=. & `var'2!=. & `var'1!=`var'2
					replace multi_flag = 1 if `var'3!=. & `var'2!=. & `var'3!=`var'2
					replace `var' =. if multi_flag==1
					
				}
				
	/* in other words: 
			
			- if there are 8 records pertaining to one episode (held in reshape variable created above), reshape so all the available values for the variable of interest read across as columns 
			
			- generate a variable that takes the maximum value from all 8 available cells across the record 
			
			- for any records where there are 2 or 3 values of the variable of interest available and they are conflicting, flag them as a multiple birth and change the value of the variable of interest to missing (can be tweaked if interested in keeping multiple births) 
			
	This is what is repeated below for each value of the reshape variable
	
	*/
				
				else if _presseq==7 {
				
					disp "_presseq==7"
					reshape wide `var', i(pregid`x' pregend_num`x' epistart_num multi_flag) j(_presseq)
					gen `var' = max(`var'1, `var'2, `var'3, `var'4, `var'5, `var'6,`var'7)
					replace multi_flag = 1 if `var'1!=. & `var'2!=. & `var'1!=`var'2
					replace multi_flag = 1 if `var'3!=. & `var'2!=. & `var'3!=`var'2
					replace `var' =. if multi_flag==1
					
				}
				
				else if _presseq==6 {
				
					reshape wide `var', i(pregid`x' pregend_num`x' epistart_num multi_flag) j(_presseq)
					gen `var' = max(`var'1, `var'2, `var'3, `var'4, `var'5, `var'6)
					replace multi_flag = 1 if `var'1!=. & `var'2!=. & `var'1!=`var'2
					replace `var' =. if multi_flag==1
					
				}
				
				else if _presseq==5 {
				
					reshape wide `var', i(pregid`x' pregend_num`x' epistart_num multi_flag) j(_presseq)
					gen `var' = max(`var'1, `var'2, `var'3, `var'4, `var'5)
					replace multi_flag = 1 if `var'1!=. & `var'2!=. & `var'1!=`var'2
					replace `var' =. if multi_flag==1
					
				}
				
				else if _presseq==4 {
				
					reshape wide `var', i(pregid`x' pregend_num`x' epistart_num multi_flag) j(_presseq)
					gen `var' = max(`var'1, `var'2, `var'3, `var'4)
					replace multi_flag = 1 if `var'1!=. & `var'2!=. & `var'1!=`var'2
					replace `var' =. if multi_flag==1
					
				}
				
				else if _presseq==3 {
				
					reshape wide `var', i(pregid`x' pregend_num`x' epistart_num multi_flag) j(_presseq)
					gen `var' = max(`var'1, `var'2, `var'3)
					replace multi_flag = 1 if `var'1!=. & `var'2!=. & `var'1!=`var'2
					replace `var' =. if multi_flag==1
					
				}
				
				else if _presseq==2 {
				
					reshape wide `var', i(pregid`x' pregend_num`x' epistart_num multi_flag) j(_presseq)
					gen `var' = max(`var'1, `var'2)
					replace multi_flag = 1 if `var'1!=. & `var'2!=. & `var'1!=`var'2
					replace `var' =. if multi_flag==1
					
				}
				
				else {
				
					disp "only one record"
					
				}
				
				sum `var'
				
				rename pregid`x' pregid
				keep patid pregid `var' multi_flag
			
			}
			
			else if _N==0 {
				
				keep patid pregid`x' multi_flag
				rename pregid`x' pregid
				
			}
			
			save "$Tempdatadir\hes_`var'_`x'.dta", replace
		
		}
		
		* Append the datasets below created for each pregnancy for each variable of interest above
		
		use "$Tempdatadir\hes_`var'_1.dta", clear
		
		forvalue y=2/11 {
		    
			append using "$Tempdatadir\hes_`var'_`y'.dta"
			
		}
		
		* Some pregnancies have had more than one record matched to them so we can apply the same process to these as above: create a reshape variable, this time on patient and pregnancy ID, create an overall variable of interest taking the maximum of the possible values and flagging those as possible multiple births that are conflicting
		
		bysort patid pregid: egen _presseq=seq()
		tab _presseq
		gsort -_presseq
		
		if _presseq>1 {
		
		reshape wide `var', i(patid pregid multi_flag) j(_presseq)
		
			gen `var' = max(`var'1, `var'2, `var'3, `var'4)
			replace multi_flag = 1 if `var'1!=. & `var'2!=. & `var'1!=`var'2
			
			keep patid pregid `var' multi_flag
		
		}
		
		else {
		    
			keep patid pregid `var' multi_flag
			
		}
		
		duplicates tag patid pregid, gen(tag)
		drop if tag==1 // drops any multiple births missed in the prior process
		drop tag
		
		save "$Tempdatadir\hes_`var'_del_singletons.dta", replace
		
	}
	
*************************************************************************************

* Merge in the derived HES data to the Pregnancy Register
	
	use "$Datadir\derived_data\pregnancy_cohort_final.dta", clear
	label variable hes_apc_e"HES Admitted Patient Care linkage - mum"
	
	* Identify babies that have linked HES data
	preserve
	
		drop if babypatid==.
		rename patid mumpatid
		rename babypatid patid 
	
		merge 1:1 patid using "$Datadir\formatted_linked_data\linkage_eligibility.dta", keep(3) keepusing(hes_apc_e) nogen
		
		rename patid babypatid
		rename mumpatid patid 
		rename hes_apc_e baby_hes_apc_e
		
		keep patid babypatid baby_hes_apc_e
		label variable baby_hes_apc_e"HES Admitted Patient Care linkage - baby"
		
		save "$Datadir\derived_data\babies_with_linked_data.dta", replace
		
	restore
	
	* Drop pregnancies that did not definitively end in a delivery
	keep if inlist(updated_outcome, 1, 2, 3, 11, 12)
	
	* Drop multiple pregnancies
	drop if multiple_ev==1
	keep if mblbabies==1 | mblbabies==.
	
	* Merge in the birthweights derived from the delivery records and rename the birthweight variables so we can compare those who have both
	merge 1:1 patid pregid using "$Tempdatadir\hes_birweit_del_singletons.dta", nogen
	rename birweit birweit_del
	label variable birweit_del"Birthweight from delivery record"
	
	merge 1:1 patid pregid using "$Tempdatadir\birweit_mbl_singletons.dta", update replace nogen
	rename birweit birweit_bir
	label variable birweit_bir"Birthweight from birth record"
	label variable birth_record"Birth record identified in HES Maternity"
	
	* Look at the deliveries that have both types of records and that differ
	count if birweit_bir==birweit_del & birweit_bir!=. // for those who have both 150,713 match...
	count if birweit_bir!=birweit_del & birweit_bir!=. & birweit_del!=. // ...and 338 don't
	
	br birweit_bir birweit_del if birweit_bir!=. & birweit_del!=. & birweit_bir!=birweit_del
	gen conflict_bw = 1 if birweit_bir!=. & birweit_del!=. & birweit_bir!=birweit_del
	replace conflict_bw = 0 if birweit_bir!=. & birweit_del!=. & birweit_bir==birweit_del 
	tab conflict_bw // 0.22% conflicting
	// n = 336 conflicting birthweights between delivery records and birth records - some are clearly typos (one or two numbers missing) but some are distinctly different - drop given so few?
	
	gen birweit = birweit_bir
	replace birweit = birweit_del if birweit_bir==.
	replace birweit =. if birweit_bir!=. & birweit_del!=. & birweit_bir!=birweit_del
	summ birweit
	label var birweit"BW from birth rec where available, from delivery rec where birth rec missing"
	
	gen bw_missing = 1 if birweit==.
	replace bw_missing = 0 if birweit!=.
	tab bw_missing
	label var bw_missing"Birthweight in neither delivery nor birth record"
	
	drop conflict_bw
	
	* Do the same with gestational length
	merge 1:1 patid pregid using "$Tempdatadir\hes_gestat_del_singletons.dta", nogen
	rename gestat gestat_del
	label variable gestat_del"Gestational length from delivery record"
	
	merge 1:1 patid pregid using "$Tempdatadir\gestat_mbl_singletons.dta", update replace nogen
	rename gestat gestat_bir
	label variable gestat_bir"Gestational length from birth record"
	
	* Look at the deliveries that have both types of records and that differ
	count if gestat_bir==gestat_del & gestat_bir!=. // for those who have both 128,132 match...
	count if gestat_bir!=gestat_del & gestat_bir!=. & gestat_del!=. // ...and 4,234 don't
	
	br gestat_bir gestat_del if gestat_bir!=. & gestat_del!=. & gestat_bir!=gestat_del
	gen diff = gestat_del - gestat_bir
	tab diff
	count if (diff>2 | diff<-2) & diff!=. // 1,270 differ by more than two weeks
	
	gen gestat_2wk = 1 if (diff>2 | diff<-2) & diff!=.
	replace gestat_2wk = 0 if diff<2 & diff>-2 & diff!=.
	tab gestat_2wk // 0.96% gestat differs by >2 weeks
	drop gestat_2wk
	
	// n = 1,270 where gestat from delivery record differs by +/- 2 weeks from birth record - drop as so few?
	
	gen gestat = gestat_bir
	replace gestat = gestat_del if gestat_bir==.
	replace gestat = round((gestat_del+gestat_bir)/2) if (diff<=2 | diff>=-2) & diff!=. // for those with differing lengths by less than two weeks take a rounded average
	replace gestat =. if (diff>2 | diff<-2) & diff!=. // change to missing if lengths differ by more than two weeks 
	drop diff
	
	gen gestat_missing = 1 if gestat==.
	replace gestat_missing = 0 if gestat!=.
	tab gestat_missing
	
	label variable gestat"GA from birth rec where available, from delivery rec where birth rec missing"
	label var gestat_missing"Gestational length in neither delivery nor birth record"
	
	* And the same with sex of the baby
	merge 1:1 patid pregid using "$Tempdatadir\hes_sexbaby_num_del_singletons.dta", nogen
	rename sexbaby_num sex_del
	label variable sex_del"Sex of the baby from delivery record"
	
	merge 1:1 patid pregid using "$Tempdatadir\sex_mbl_singletons.dta", update replace nogen
	rename sex sex_bir
	label variable sex_bir"Sex of the baby from birth record"
	
	* Look at the deliveries that have both types of records and that differ
	count if sex_bir==sex_del & sex_bir!=. // for those who have both 13,135 match...
	count if sex_bir!=sex_del & sex_bir!=. & sex_del!=. // ...and 48 don't
	
	br sex_bir sex_del if sex_bir!=. & sex_del!=. & sex_bir!=sex_del
	// n = 48 sexes don't match up - drop as so few
	gen conflict_sex = 1 if sex_bir!=. & sex_del!=. & sex_bir!=sex_del
	replace conflict_sex = 0 if sex_del!=. & sex_bir!=. & sex_bir==sex_del
	tab conflict_sex // 0.36%
	
	gen sex = sex_bir
	replace sex = sex_del if sex_bir==.
	replace sex =. if sex_bir!=. & sex_del!=. & sex_bir!=sex_del 
	
	drop conflict_sex
	
	gen sex_missing = 1 if sex==.
	replace sex_missing = 0 if sex!=.
	tab sex_missing
	
	label var sex"Sex from birth rec where available, from delivery rec where birth rec missing"
	label var sex_missing"Baby sex in neither delivery nor birth record"
	
	*drop if multi_flag==1 // n = 691 likely to be a multiple pregnancy as conflicting birthweights on the same delivery record indicative of a multiple birth (on advice from Katie)
	
	replace babypatid = -_n if missing(babypatid)
	
	merge m:1 patid using "$Datadir\formatted_linked_data\linkage_eligibility.dta", keep(3) keepusing(hes_apc_e) nogen
	merge 1:1 babypatid patid using "$Datadir\derived_data\babies_with_linked_data.dta", keep(1 3) nogen
	
	replace babypatid =. if babypatid<0
	
	count // n = 734,228 deliveries in the Pregnancy Register
	
	count if hes_apc_e==1 // 355,859 deliveries (among 262,360 mums) in the PregReg with HES data
	codebook patid if hes_apc_e==1
	
	count if baby_hes_apc_e==1 // 292,080 babies in the PregReg with HES data
	
	tab sex_missing, m 	
	tab sex_missing if baby_hes_apc_e==1, m
	* 265,382 with sex (36% of PregReg, 76% of those with linked data)
	sum birweit
	tab bw_missing
	tab bw_missing if baby_hes_apc_e==1
	* 274,014 with a birthweight (37% of PregReg, 78% with linked data)
	sum gestat
	tab gestat_missing
	tab gestat_missing if baby_hes_apc_e==1, m
	* 251,189 with a gestational length (34% of PregReg, 72% with linked data)
	
	gen delivery_record = 1 if (birweit!=. | sex!=. | gestat!=.) & birth_record==.
	
	tab birth_record 	// n = 231,616 with a birth record
	tab delivery_record // n = 76,031 with a delivery record only
	label var delivery_record"Delivery record identified in HES Maternity"
	
	save "$Datadir\derived_data\pregreg_bw_gl.dta", replace
	
*************************************************************************************

* Onset of delivery from birth records

	use "$Datadir\derived_data\pregnancy_cohort_final.dta", clear
	drop if babypatid==.
	count
	tab updated_outcome
	tab mblbabies
	drop if mblbabies>1 | multiple_ev==1
	
	rename patid mumpatid
	merge 1:1 mumpatid babypatid using "$Datadir\formatted_linked_data\mbl_baby.dta", keep(3) nogen
	keep mumpatid babypatid pregid pregend_num
	
	* n = 580,349 singleton babies in the Mother Baby Link 
	
	* Renaming babypatid patid because we want to merge to the birth records that we identified in HES Maternity (therefore affiliated with baby's patid)
	rename babypatid patid
	
	merge 1:m patid using "$Tempdatadir\birth_episodes.dta", keep(3) nogen
	
	tab delonset
	
	keep mumpatid patid pregid delonset
	
	rename patid babypatid
	rename mumpatid patid
	
	duplicates report
	duplicates drop
	
	duplicates tag patid pregid, gen(dup)
	tab dup
	sort patid pregid
	br if dup>0
	
	bysort patid pregid: egen seq=seq()
	drop dup
	
	reshape wide delonset, i(patid pregid babypatid) j(seq)
	
	replace delonset1 = delonset2 if delonset1==. & delonset2!=.
	replace delonset2 =. if delonset1==delonset2
	
	replace delonset1 =. if delonset1!=delonset2 & delonset2!=.
	replace delonset1 =. if delonset1!=delonset3 & delonset3!=.
	
	drop delonset2 delonset3
	
	rename delonset1 delonset
	
	gen birth_record = 1
	
	save "$Tempdatadir\delonset_birth_records.dta", replace
	
* Onset of delivery from delivery records 

		forvalues x=1/11 { // based on reshape variable value for your dataset
	
			use "$Tempdatadir\delivery_episodes.dta", clear
			
			gen diff = pregend_num`x' - epistart_num
			summ diff
			
			keep if diff < 175 & diff >-175
			
			tab delonset
			
			keep patid pregid`x' delonset
			rename pregid`x' pregid
			
			gen delivery_record = 1
			
			save "$Tempdatadir\delonset_del_records_`x'.dta", replace
		
		}
		
	use "$Tempdatadir\delonset_del_records_1.dta", clear
	
	forvalues x=2/11 {
		
		append using "$Tempdatadir\delonset_del_records_`x'.dta"
		
	}
	
	duplicates report
	duplicates drop
	
	duplicates tag patid pregid, gen(dup)
	tab dup
	br if dup>0 // conflicts - change all to missing ~20 of them
	
	replace delonset =. if dup>0
	duplicates drop patid pregid, force
	drop dup
	
	merge 1:1 patid pregid using "$Tempdatadir\delonset_birth_records.dta", update replace nogen
	
	drop if delonset==.
	
	tab delonset
	order patid pregid babypatid
	
	save "$Datadir\derived_data\labour_induction.dta", replace
	
*************************************************************************************

* Delete unnecessary datasets

	erase "$Tempdatadir\icd_10_deliveries.dta"
	erase "$Tempdatadir\hes_birweit_del_singletons.dta"
	erase "$Tempdatadir\birweit_mbl_singletons.dta"
	erase "$Tempdatadir\hes_gestat_del_singletons.dta"
	erase "$Tempdatadir\gestat_mbl_singletons.dta"
	erase "$Tempdatadir\hes_sexbaby_num_del_singletons.dta"
	erase "$Tempdatadir\sex_mbl_singletons.dta"
	
*************************************************************************************
