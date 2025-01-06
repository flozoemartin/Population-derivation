********************************************************************************

* Scales used to measure severity of antidepressant indications to define those with mild, moderate, or severe indication in the year before pregnancy

* Author: Flo Martin

* Date: 10/01/2023

********************************************************************************

* Datasets created by this do-file

* - 	$Tempdatadir\severity_before_and_during_preg.dta

********************************************************************************

* Start logging

	log using "$Logdir\5_indications\3_scales.txt", replace
	
********************************************************************************

* Lift all events of depression from the Clinical and Referral Files

	forvalues x=0/11 {
		
		use "$Datadir\formatted_cprd_data\Clinical_`x'.dta", clear
			
		merge m:1 medcode using "$Tocheckdir\dep_anx_ed_pain_scales.dta", keep(3) nogen
		keep if dep_anx==1
			
		sort patid
			
		save "$Tempdatadir\scales_Clin_`x'.dta", replace
		
	}
	
	use "$Datadir\formatted_cprd_data\All_Referral.dta", clear
			
	merge m:1 medcode using "$Tocheckdir\dep_anx_ed_pain_scales.dta", keep(3) nogen
	keep if dep_anx==1
			
	sort patid
			
	save "$Tempdatadir\scales_Ref.dta", replace
	
	use "$Tempdatadir\scales_Ref.dta", clear
	
	forvalues x=0/11 {
		
		append using "$Tempdatadir\scales_Clin_`x'.dta"
		
	}
	
	save "$Tempdatadir\all_scales.dta", replace
	
	count // 885,333 events
	codebook patid // among 385,385 patients
	
	replace adid = -_n if adid==0
	
	merge m:1 patid adid using "$Datadir\formatted_cprd_data\All_Additional.dta", nogen keep(3) // keep those with a score 
	
	tab data1, m
	
	gen score = real(data1)
	tab score
	
	gen severity =.
	
	* Beck depression inventory second edition score - depression
	
	replace severity = 0 if (score>=0 & score<=13) & desc=="beck depression inventory second edition score"
	replace severity = 1 if (score>=14 & score<=19) & desc=="beck depression inventory second edition score"
	replace severity = 2 if (score>=20 & score<=28) & desc=="beck depression inventory second edition score"
	replace severity = 3 if (score>=29 & score!=.) & desc=="beck depression inventory second edition score"
	
	* CORE-10 - distress
	
	replace severity = 0 if (score>=0 & score<=5) & desc=="core-10 score"
	replace severity = 1 if (score>=6 & score<=10) & desc=="core-10 score"
	replace severity = 2 if (score>=15 & score<=19) & desc=="core-10 score"
	replace severity = 3 if (score>=20 & score!=.) & desc=="core-10 score"
	
	* EPDS - postnatal depression
	
	replace severity = 0 if (score>=0 & score<=6) & strmatch(desc, "*edinburgh postnatal depression scale")
	replace severity = 1 if (score>=7 & score<=13) & strmatch(desc, "*edinburgh postnatal depression scale")
	replace severity = 2 if (score>=14 & score<=19) & strmatch(desc, "*edinburgh postnatal depression scale")
	replace severity = 3 if (score>=20 & score!=.) & strmatch(desc, "*edinburgh postnatal depression scale")
	
	* GAD - anxiety
	
	replace severity = 0 if (score>=0 & score<=4) & (strmatch(desc, "gad-7*") | strmatch(desc, "generalised anxiety disorder*"))
	replace severity = 1 if (score>=5 & score<=9) & (strmatch(desc, "gad-7*") | strmatch(desc, "generalised anxiety disorder*"))
	replace severity = 2 if (score>=10 & score<=14) & (strmatch(desc, "gad-7*") | strmatch(desc, "generalised anxiety disorder*"))
	replace severity = 3 if (score>=15 & score!=.) & (strmatch(desc, "gad-7*") | strmatch(desc, "generalised anxiety disorder*"))
	
	* HADS - depression and anxiety 
	
	replace severity = 0 if (score>=0 & score<=4) & strmatch(desc, "had*")
	replace severity = 1 if (score>=5 & score<=9) & strmatch(desc, "had*")
	replace severity = 2 if (score>=10 & score<=14) & strmatch(desc, "had*")
	replace severity = 3 if (score>=15 & score!=.) & strmatch(desc, "had*")
	
	* Hamilton Depression Rating Scale - depression
	
	replace severity = 0 if (score>=0 & score<=7) & strmatch(desc, "*hamilton*")
	replace severity = 1 if (score>=8 & score<=16) & strmatch(desc, "*hamilton*")
	replace severity = 2 if (score>=17 & score<=23) & strmatch(desc, "*hamilton*")
	replace severity = 3 if (score>=24 & score!=.) & strmatch(desc, "*hamilton*")
	
	* MADRS - depressive episodes
	
	replace severity = 0 if (score>=0 & score<=6) & strmatch(desc, "madrs*")
	replace severity = 1 if (score>=7 & score<=19) & strmatch(desc, "madrs*")
	replace severity = 2 if (score>=20 & score<=34) & strmatch(desc, "madrs*")
	replace severity = 3 if (score>=35 & score!=.) & strmatch(desc, "madrs*")
	
	* PHQ-9 - depression
	
	replace severity = 0 if (score>=0 & score<=4) & strmatch(desc, "*phq-9*")
	replace severity = 1 if (score>=5 & score<=9) & strmatch(desc, "*phq-9*")
	replace severity = 2 if (score>=10 & score<=14) & strmatch(desc, "*phq-9*")
	replace severity = 3 if (score>=15 & score!=.) & strmatch(desc, "*phq-9*")
	
	keep if severity!=.
	
	label define sev_lb 0"None" 1"Mild" 2"Moderate" 3"Severe"
	label values severity sev_lb
	tab severity
	
	keep patid eventdate_num medcode readcode desc enttype adid score severity
	
	save "$Tempdatadir\scale scores.dta", replace
	
	use "$Datadir\derived_data\pregnancy_cohort_final.dta", clear
	bysort patid: gen bign = _N
	summ bign // n = 14 max number of pregnancies
	local maxpreg = r(max)
	sort patid pregstart_num
	drop bign
	
* Creat binary variables for each patient's pregnancies whether they had a code for depression or anxiety in the periods of interest
		
		* 12 months before pregnancy until the end of pregnancy
	
		forvalues n=1/`maxpreg' {
			
			use "$Datadir\derived_data\pregnancy_cohort_final_`n'.dta", clear
			keep patid pregid pregstart_num pregend_num
			
			merge 1:m patid using "$Tempdatadir\scale scores.dta", keep(3) nogen
			
			keep if eventdate_num>=pregstart_num-365 & eventdate_num<pregstart_num
			
			if _N>0 {
			
				keep patid pregid eventdate_num pregstart_num severity
				
				duplicates drop
				
			}
			
			else if _N==0 {	
				
				keep patid pregid
				
			}
			
			save "$Tempdatadir\scores_12mo_`n'.dta", replace
		
		}
		
		use "$Tempdatadir\scores_12mo_1.dta", clear
		
		forvalues n=2/`maxpreg' {
			
			append using "$Tempdatadir\scores_12mo_`n'.dta"
			
		}
		
		sort patid pregid eventdate_num
		by patid pregid: egen _seq = seq()
		
		count // 40,630 events
		codebook pregid // among 25,981 pregnancies
		codebook patid // among 22,911 patients
		
		save "$Tempdatadir\scores_12mo.dta", replace
		
* Define periods of interest
	
	gen prepreg_3month_num  = round(pregstart_num-3*30.5)
	gen prepreg_6month_num  = round(pregstart_num-6*30.5)
	gen prepreg_9month_num  = round(pregstart_num-9*30.5)
	gen prepreg_12month_num = round(pregstart_num-12*30.5)
	
	format %td prepreg_*
	
		gen flagl_prepreg_12_9 = severity if (prepreg_12month_num <= eventdate_num & eventdate_num < prepreg_9month_num) 
		gen flagm_prepreg_9_6  		= severity if (prepreg_9month_num <= eventdate_num & eventdate_num < prepreg_6month_num) 
		gen flagn_prepreg_6_3  		= severity if (prepreg_6month_num <= eventdate_num & eventdate_num < prepreg_3month_num)
		gen flago_prepreg_3_0  		= severity if (prepreg_3month_num <= eventdate_num & eventdate_num < pregstart_num) 
	
		label values flagl_prepreg_12_9 sev_lb
		label values flagm_prepreg_9_6 sev_lb
		label values flagn_prepreg_6_3 sev_lb
		label values flago_prepreg_3_0 sev_lb
		
		keep patid pregid flag* eventdate_num
		
		by patid pregid: egen _seq = seq()
		
		reshape wide flag* eventdate_num, i(patid pregid) j(_seq)
		
		gen flagl = max(flagl_prepreg_12_91, flagl_prepreg_12_92, flagl_prepreg_12_93, flagl_prepreg_12_94, flagl_prepreg_12_95, flagl_prepreg_12_96, flagl_prepreg_12_97, flagl_prepreg_12_98, flagl_prepreg_12_99, flagl_prepreg_12_910, flagl_prepreg_12_911, flagl_prepreg_12_912, flagl_prepreg_12_913, flagl_prepreg_12_914, flagl_prepreg_12_915, flagl_prepreg_12_916, flagl_prepreg_12_917)
		gen flagm = max(flagm_prepreg_9_61, flagm_prepreg_9_62, flagm_prepreg_9_63, flagm_prepreg_9_64, flagm_prepreg_9_65, flagm_prepreg_9_66, flagm_prepreg_9_67, flagm_prepreg_9_68, flagm_prepreg_9_69, flagm_prepreg_9_610, flagm_prepreg_9_611, flagm_prepreg_9_612, flagm_prepreg_9_613, flagm_prepreg_9_614, flagm_prepreg_9_615, flagm_prepreg_9_616, flagm_prepreg_9_617)
		gen flagn = max(flagn_prepreg_6_31, flagn_prepreg_6_32, flagn_prepreg_6_33, flagn_prepreg_6_34, flagn_prepreg_6_35, flagn_prepreg_6_36, flagn_prepreg_6_37, flagn_prepreg_6_38, flagn_prepreg_6_39, flagn_prepreg_6_310, flagn_prepreg_6_311, flagn_prepreg_6_312, flagn_prepreg_6_313, flagn_prepreg_6_314, flagn_prepreg_6_315, flagn_prepreg_6_316, flagn_prepreg_6_317)
		gen flago = max(flago_prepreg_3_01, flago_prepreg_3_02, flago_prepreg_3_03, flago_prepreg_3_04, flago_prepreg_3_05, flago_prepreg_3_06, flago_prepreg_3_07, flago_prepreg_3_08, flago_prepreg_3_09, flago_prepreg_3_010, flago_prepreg_3_011, flago_prepreg_3_012, flago_prepreg_3_013, flago_prepreg_3_014, flago_prepreg_3_015, flago_prepreg_3_016, flago_prepreg_3_017)
		
		keep patid pregid flagl flagm flagn flago
		
		label values flagl sev_lb
		label values flagm sev_lb
		label values flagn sev_lb
		label values flago sev_lb
		
		save "$Tempdatadir\severity_12mo_preg.dta", replace
		
		* 12 months before pregnancy until the end of pregnancy
	
		forvalues n=1/`maxpreg' {
			
			use "$Datadir\derived_data\pregnancy_cohort_final_`n'.dta", clear
			keep patid pregid pregstart_num secondtrim_num thirdtrim_num pregend_num
			
			merge 1:m patid using "$Tempdatadir\scale scores.dta", keep(3) nogen
			
			keep if eventdate_num>=pregstart_num & eventdate_num<pregend_num
			
			if _N>0 {
			
				keep patid pregid eventdate_num pregstart_num pregend_num secondtrim_num thirdtrim_num severity
				
				duplicates drop
				
			}
			
			else if _N==0 {	
				
				keep patid pregid
				
			}
			
			save "$Tempdatadir\scores_preg_`n'.dta", replace
		
		}
		
		use "$Tempdatadir\scores_preg_1.dta", clear
		
		forvalues n=2/`maxpreg' {
			
			append using "$Tempdatadir\scores_preg_`n'.dta"
			
		}
		
		sort patid pregid eventdate_num
		by patid pregid: egen _seq = seq()
		
		count // 40,630 events
		codebook pregid // among 25,981 pregnancies
		codebook patid // among 22,911 patients
		
		save "$Tempdatadir\scores_preg.dta", replace
		
	
		gen flaga_preg_firsttrim  	= severity if ((secondtrim_num!=. & pregstart_num <= eventdate_num & eventdate_num < secondtrim_num)) | ((secondtrim_num==. & pregstart_num <= eventdate_num & eventdate_num < pregend_num)) 
		gen flagb_preg_secondtrim 	= severity if ((secondtrim_num!=. & thirdtrim_num!=. & secondtrim_num <= eventdate_num & eventdate_num < thirdtrim_num)) | ((secondtrim_num!=. & thirdtrim_num==. & secondtrim_num <= eventdate_num & eventdate_num < pregend_num)) 
		gen flagc_preg_thirdtrim  	= severity if (thirdtrim_num!=. & thirdtrim_num <= eventdate_num & eventdate_num < pregend_num) 
	
		label values flaga_preg_firsttrim sev_lb
		label values flagb_preg_secondtrim sev_lb
		label values flagc_preg_thirdtrim sev_lb
		
		keep patid pregid flag*
		
		by patid pregid: egen _seq = seq()
		
		reshape wide flag*, i(patid pregid) j(_seq)
		
		gen flaga = max(flaga_preg_firsttrim1, flaga_preg_firsttrim2, flaga_preg_firsttrim3, flaga_preg_firsttrim4, flaga_preg_firsttrim5, flaga_preg_firsttrim6, flaga_preg_firsttrim7, flaga_preg_firsttrim8, flaga_preg_firsttrim9, flaga_preg_firsttrim10, flaga_preg_firsttrim11, flaga_preg_firsttrim12, flaga_preg_firsttrim13, flaga_preg_firsttrim14, flaga_preg_firsttrim15, flaga_preg_firsttrim16)
		gen flagb = max(flagb_preg_secondtrim1, flagb_preg_secondtrim2, flagb_preg_secondtrim3, flagb_preg_secondtrim4, flagb_preg_secondtrim5, flagb_preg_secondtrim6, flagb_preg_secondtrim7, flagb_preg_secondtrim8, flagb_preg_secondtrim9, flagb_preg_secondtrim10, flagb_preg_secondtrim11, flagb_preg_secondtrim12, flagb_preg_secondtrim13, flagb_preg_secondtrim14, flagb_preg_secondtrim15, flagb_preg_secondtrim16)
		gen flagc = max(flagc_preg_thirdtrim1, flagc_preg_thirdtrim2, flagc_preg_thirdtrim3, flagc_preg_thirdtrim4, flagc_preg_thirdtrim5, flagc_preg_thirdtrim6, flagc_preg_thirdtrim7, flagc_preg_thirdtrim8, flagc_preg_thirdtrim9, flagc_preg_thirdtrim10, flagc_preg_thirdtrim11, flagc_preg_thirdtrim12, flagc_preg_thirdtrim13, flagc_preg_thirdtrim14, flagc_preg_thirdtrim15, flagc_preg_thirdtrim16)
		
		label values flaga sev_lb
		label values flagb sev_lb
		label values flagc sev_lb
		
		keep patid pregid flaga flagb flagc
		
		save "$Tempdatadir\severity_preg.dta", replace
		
		use "$Tempdatadir\severity_12mo_preg.dta", clear
		merge 1:1 patid pregid using "$Tempdatadir\severity_preg.dta", nogen
		
		save "$Tempdatadir\severity_before_and_during_preg.dta", replace

*******************************************************************************

* Stop logging

	log close
	
********************************************************************************
