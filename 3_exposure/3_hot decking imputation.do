********************************************************************************

* Hot-decking imputation for missing values in the prescription data 

* Author: Flo Martin (amended from PREPArE project)

* Date: 21/04/2022

********************************************************************************

* Datasets generated by this do-file

	* AD_pxn_events_from_All_Therapy_clean.dta - all the missing prescription data for prescription of antidepressants that occur within a year of patient's first pregnancy or after in the pregnancy register imputed

********************************************************************************

* Start logging

	log using "$Logdir\3_exposure\3_hot decking imputation.txt", replace
	
********************************************************************************

* Import cleaned prescription data ready for hot-decking imputation

	use "$Tempdatadir\AD_pxn_events_from_All_Therapy_for_imputation.dta", clear
	
/* Data cleaning process
********************************************************************************

- 1 Identify all implausible values of quantity and daily dose prescribed 
	- 1.1 Daily dose
		- 0 tablets/capsules a day (already converted to missing in the cleaning script)
		- > 10 tablets/capsules a day
	- 1.2 Quantity
		- < 7 tablets/capsules
		- > 280 capsules = 10 capsules a day (maximum daily dose) for 4 weeks
- 2 Set all implausible values to missing
- 3 Use the hotdecking procedure to impute missing values for quantity and daily dose prescribed

********************************************************************************/

	tab clean_daily_dose, m
	replace clean_daily_dose =. if clean_daily_dose >10 // 117 values set to missing
	
	tab qty, m
	replace qty =. if qty <7 | qty >280					// 2 values set to missing
	
	* Changing these to missing as extensive cleaning occurred in previous do-file and no more useful information can be obtained from other variables
	
	count							// 3,919,624 (100%)
	count if qty ==.				// 29,555 (0.6%)
	count if clean_daily_dose ==.	// 779,547 (19.7%)
	
	* Create a flag to indicate imputed values

	gen flag_imputed_daily_dose = 1 if clean_daily_dose==.
	gen flag_imputed_qty = 1 if qty==.
	
	gen dose_unit2 = .
	replace dose_unit2=1 if inlist(dose_unit, "CAP", "TAB")
	replace dose_unit2=2 if inlist(dose_unit, "DROPS")
	replace dose_unit2=3 if inlist(dose_unit, "ML")
	
	tab dose_unit dose_unit2, m
	
********************************************************************************
	
* Impute values for daily dose at decreasing levels of specificity
	
	* If daily_dose missing, impute modal daily dose within dose_unit2/patient/prodcode/qty

	bysort dose_unit2 patid prodcode qty: egen modal_daily_dose = mode(clean_daily_dose) 
	replace clean_daily_dose = modal_daily_dose if clean_daily_dose==. 
	count if clean_daily_dose==. 
	
	* If daily_dose still missing, impute modal daily_dose for dose_unit2/prodcode/qty in any patient

	bysort dose_unit2 prodcode qty: egen modal_daily_dose2 = mode(clean_daily_dose) 
	replace clean_daily_dose = modal_daily_dose2 if clean_daily_dose==. & modal_daily_dose==. 
	count if clean_daily_dose==.
	
	* If daily_dose still missing, impute modal daily_dose for dose_unit2/prodcode in any patient

	bysort dose_unit2 prodcode: egen modal_daily_dose3 = mode(clean_daily_dose) 
	replace clean_daily_dose = modal_daily_dose3 if clean_daily_dose==. & modal_daily_dose==. & modal_daily_dose2==. 
	count if clean_daily_dose==. 
	
	* If daily_dose then still missing, impute modal daily_dose for dose_unit2/prodname in any patient

	bysort dose_unit2 productname: egen modal_daily_dose4 = mode(clean_daily_dose) 
	replace daily_dose = modal_daily_dose4 if clean_daily_dose==. & modal_daily_dose==. & modal_daily_dose2==. & modal_daily_dose3==.
	count if clean_daily_dose==. 
	
	* If daily_dose then still missing, impute modal daily_dose for dose_unit2/aedclass in any patient

	bysort dose_unit2 class: egen modal_daily_dose5 = mode(clean_daily_dose) 
	replace clean_daily_dose = modal_daily_dose5 if clean_daily_dose==. & modal_daily_dose==. & modal_daily_dose2==. & modal_daily_dose3==. & modal_daily_dose4==. 
	count if clean_daily_dose==.
	
********************************************************************************

* Check 

	count							// 3,919,624 (100%)
	count if qty ==.				// 29,555 (0.6%)
	count if clean_daily_dose ==.	// 0 (0%)	
	
********************************************************************************

* Impute values for quantity

	* If qty missing, impute modal qty within dose_unit2/patient/prodcode/daily_dose
	
	bysort dose_unit2 patid prodcode clean_daily_dose: egen modal_qty = mode(qty) 
	replace qty = modal_qty if qty==. 
	count if qty==. 
	
	*If qty still missing, impute modal qty for dose_unit2/prodcode/daily_dose in any patient

	bysort dose_unit2 prodcode clean_daily_dose: egen modal_qty2 = mode(qty) 
	replace qty = modal_qty2 if qty==. & modal_qty==. 
	count if qty==.
	
	* If qty still missing, impute modal qty for dose_unit2/prodcode in any patient

	bysort dose_unit2 prodcode: egen modal_qty3 = mode(qty) 
	replace qty = modal_qty3 if qty==. & modal_qty==. & modal_qty2==. 
	count if qty==. 
	
	* If qty then still missing, impute modal qty for dose_unit2/prodname in any patient

	bysort dose_unit2 productname: egen modal_qty4 = mode(qty) 
	replace qty = modal_qty4 if qty==. & modal_qty==. & modal_qty2==. & modal_qty3==.
	count if qty==. 
	
	* If qty then still missing, impute modal qty for dose_unit2/aedclass in any patient

	bysort dose_unit2 class: egen modal_qty5 = mode(qty) 
	replace qty = modal_qty5 if qty==. & modal_qty==. & modal_qty2==. & modal_qty3==. & modal_qty4==. 
	count if qty==.
	
********************************************************************************

* Check 

	count							// 3,919,624 (100%)
	count if qty ==.				// 0 (0%)
	count if clean_daily_dose ==.	// 0 (0%)	
	
********************************************************************************

* Update flag to indicate imputed values

	replace flag_imputed_daily_dose = . if clean_daily_dose ==.
	replace flag_imputed_qty = . if qty ==.

	tab flag_imputed_daily_dose, miss
	tab flag_imputed_qty, miss
	
* Drop any unneeded variables

	drop /*
	*/ dose_unit2 /*
	*/ modal_daily_dose modal_daily_dose2 modal_daily_dose3 modal_daily_dose4 modal_daily_dose5 /*
	*/ modal_qty modal_qty2 modal_qty3 modal_qty4 modal_qty5
	
********************************************************************************

* Summary statistics for daily dose 
	
	summ clean_daily_dose, de
	summ clean_daily_dose if flag_imputed_daily_dose==., de
	summ clean_daily_dose if flag_imputed_daily_dose==1, de

	twoway /*
	*/ histogram clean_daily_dose if flag_imputed_daily_dose==., width(2) blcolor(red) bfcolor(none) fraction || /*
	*/ histogram clean_daily_dose if flag_imputed_daily_dose==1, width(2) bfcolor(none) blcolor(blue) fraction legend(order(1 "Actual" 2 "Imputed"))

	twoway /*
	*/ histogram daily_dose if flag_imputed_daily_dose==. & daily_dose<=10, width(1) blcolor(red) bfcolor(none) fraction || /*
	*/ histogram daily_dose if flag_imputed_daily_dose==1 & daily_dose<=10, width(1) bfcolor(none) blcolor(blue) fraction legend(order(1 "Actual" 2 "Imputed"))

 
* Summary statistics for quantity

	summ qty, de
	summ qty if flag_imputed_qty==., de
	summ qty if flag_imputed_qty==1, de

	twoway /*
	*/ histogram qty if flag_imputed_qty==., width(2) blcolor(red) bfcolor(none) fraction || /*
	*/ histogram qty if flag_imputed_qty==1, width(2) bfcolor(none) blcolor(blue) fraction legend(order(1 "Actual" 2 "Imputed"))

	twoway /*
	*/ histogram qty if flag_imputed_qty==. & qty<=336, width(2) blcolor(red) bfcolor(none) fraction || /*
	*/ histogram qty if flag_imputed_qty==1 & qty<=336, width(2) bfcolor(none) blcolor(blue) fraction legend(order(1 "Actual" 2 "Imputed"))
	
********************************************************************************

* Save this as a temporary dataset

	save "$Tempdatadir\AD_pxn_events_from_All_Therapy_imputed.dta", replace
	
********************************************************************************
	
	use "$Tempdatadir\AD_pxn_events_from_All_Therapy_imputed.dta", clear
	
* Now, we can generate prescription length from qty & clean_daily_dose that have been imputed
			
	gen flag_imputed_prescr_length = .
	replace flag_imputed_prescr_length = 1 if flag_imputed_daily_dose==1 | flag_imputed_qty==1
	
* Generate new prescription length and quality check the imputed data
* Prescription duration: Quantity (total number tablets) / daily dose (tablets per day)

	gen prescr_length2 = ceil(qty / clean_daily_dose) // rounding up to the next whole number
	label var prescr_length2 "Prescription duration in days (using imputed qty & clean_daily_dose)"
	
	tab prescr_length2, m // missing n=0 range 2 - 375
	
	summ prescr_length2, de
	summ prescr_length2 if flag_imputed_prescr_length==., de
	summ prescr_length2 if flag_imputed_prescr_length==1, de
	
	* Censor at a lowest value of 2 days and highest value of 360 days

	replace prescr_length2 = 2 if prescr_length2<2
	replace prescr_length2 = 360 if prescr_length2>360
	
	summ prescr_length2, de
	summ prescr_length2 if flag_imputed_prescr_length==., de
	summ prescr_length2 if flag_imputed_prescr_length==1, de
	
	twoway /*
	*/ histogram prescr_length2 if flag_imputed_prescr_length==., width(4) blcolor(red) bfcolor(none) fraction || /*
	*/ histogram prescr_length2 if flag_imputed_prescr_length==1, width(2) bfcolor(none) blcolor(blue) fraction legend(order(1 "Actual" 2 "Imputed"))
	
********************************************************************************

* Save this as a temporary dataset

	save "$Tempdatadir\AD_pxn_events_from_All_Therapy_prescr_length.dta", replace
	
********************************************************************************

	use "$Tempdatadir\AD_pxn_events_from_All_Therapy_prescr_length.dta", clear
	
* Now, we want to define the daily dose in mg based on imputed data

* Create flag to indicate daily dose based on imputed data

	gen flag_imputed_dd_mg = .
	replace flag_imputed_dd_mg = 1 if flag_imputed_daily_dose==1 
	
* Calculate daily dose in mg

	gen dd_mg=clean_daily_dose*dose_mg
	
	sum dd_mg, detail	// range 2 - 3000 (3g relate to tryptophan dosed at 500mg), missing all nefazodone starter packs
	sum dd_mg if flag_imputed_dd_mg==., de
	sum dd_mg if flag_imputed_dd_mg==1, de
	
	twoway /*
	*/ histogram dd_mg if flag_imputed_dd_mg==., width(4) blcolor(red) bfcolor(none) fraction || /*
	*/ histogram dd_mg if flag_imputed_dd_mg==1, width(2) bfcolor(none) blcolor(blue) fraction legend(order(1 "Actual" 2 "Imputed"))

	twoway /*
	*/ histogram dd_mg if flag_imputed_dd_mg==. & dd_mg<3200 , width(4) blcolor(red) bfcolor(none) fraction || /*
	*/ histogram dd_mg if flag_imputed_dd_mg==1 & dd_mg<3200, width(2) bfcolor(none) blcolor(blue) fraction legend(order(1 "Actual" 2 "Imputed"))
	
	tab dd_mg, m // 58 (<0.0001%) missing
	label var dd_mg "Daily dose (mg) taken per day"

********************************************************************************

* Save this as a temporary dataset

	save "$Tempdatadir\AD_pxn_events_from_All_Therapy_dd_mg.dta", replace
	
********************************************************************************

	use "$Tempdatadir\AD_pxn_events_from_All_Therapy_dd_mg.dta", clear
	
* Now, we can estimate the prescription end dates and produce the final cleaned dataset containing all relevant antidepressant prescriptions

	gen presc_startdate_num = eventdate_num
	gen presc_enddate_num = eventdate_num + prescr_length2 // one prescription doesn't have an eventdate_num - use sysdate_num here? Check with HF
	format %td presc_startdate_num presc_enddate_num 
	
********************************************************************************

* Data management required for defining exposures

	tab productname if drugsubstance==""
	
	foreach drug in amitriptyline amoxapine clomipramine trimipramine maprotiline protriptyline trazodone imipramine {
		
		replace drugsubstance = "`drug'" if regexm(productname, "`drug'") & drugsubstance==""
		
	}
	
	foreach x in agomelatine amitriptyline amoxapine butriptyline citalopram clomipramine desipramine dosulepin doxepin duloxetine escitalopram fluoxetine fluvoxamine imipramine iprindole iproniazide isocarboxazid lofepramine maprotiline mianserin mirtazapine moclobemide nefazodone nortriptyline paroxetine phenelzine protriptyline reboxetine sertraline tranylcypromine trazodone trimipramine tryptophan venlafaxine vortioxetine {
		
				replace drugsubstance = "`x'" if drugsubstance=="`x' hydrochloride"
				replace drugsubstance = "`x'" if drugsubstance=="`x' hydrobromide"
				replace drugsubstance = "`x'" if drugsubstance=="`x' sulfate"
				replace drugsubstance = "`x'" if drugsubstance=="`x' maleate"
				replace drugsubstance = "`x'" if drugsubstance=="`x' mesilate"
				replace drugsubstance = "`x'" if drugsubstance=="`x' oxalate"
		
			}
	
		gen drugsubstance_str = "1" if drugsubstance=="agomelatine"
		replace drugsubstance_str = "2" if drugsubstance=="amitriptyline"
		replace drugsubstance_str = "3" if drugsubstance=="amoxapine"
		replace drugsubstance_str = "4" if drugsubstance=="butriptyline"
		replace drugsubstance_str = "5" if drugsubstance=="citalopram"
		replace drugsubstance_str = "6" if drugsubstance=="clomipramine"
		replace drugsubstance_str = "7" if drugsubstance=="desipramine"
		replace drugsubstance_str = "8" if drugsubstance=="dosulepin"
		replace drugsubstance_str = "9" if drugsubstance=="doxepin"
		replace drugsubstance_str = "10" if drugsubstance=="duloxetine"
		replace drugsubstance_str = "11" if drugsubstance=="escitalopram"
		replace drugsubstance_str = "12" if drugsubstance=="fluoxetine"
		replace drugsubstance_str = "13" if drugsubstance=="fluvoxamine"
		replace drugsubstance_str = "14" if drugsubstance=="imipramine"
		replace drugsubstance_str = "15" if drugsubstance=="iproniazide"
		replace drugsubstance_str = "16" if drugsubstance=="iprindole"
		replace drugsubstance_str = "17" if drugsubstance=="isocarboxazid"
		replace drugsubstance_str = "18" if drugsubstance=="lofepramine"
		replace drugsubstance_str = "19" if drugsubstance=="maprotiline"
		replace drugsubstance_str = "20" if drugsubstance=="mianserin"
		replace drugsubstance_str = "21" if drugsubstance=="mirtazapine"
		replace drugsubstance_str = "22" if drugsubstance=="moclobemide"
		replace drugsubstance_str = "23" if drugsubstance=="nefazodone"
		replace drugsubstance_str = "24" if drugsubstance=="nortriptyline"
		replace drugsubstance_str = "25" if drugsubstance=="paroxetine"
		replace drugsubstance_str = "26" if drugsubstance=="phenelzine"
		replace drugsubstance_str = "27" if drugsubstance=="protriptyline"
		replace drugsubstance_str = "28" if drugsubstance=="reboxetine"
		replace drugsubstance_str = "29" if drugsubstance=="sertraline"
		replace drugsubstance_str = "30" if drugsubstance=="tranylcypromine"
		replace drugsubstance_str = "31" if drugsubstance=="trazodone"
		replace drugsubstance_str = "32" if drugsubstance=="trimipramine"
		replace drugsubstance_str = "33" if drugsubstance=="tryptophan"
		replace drugsubstance_str = "34" if drugsubstance=="venlafaxine"
		replace drugsubstance_str = "35" if drugsubstance=="vortioxetine"
	
		gen drugsubstance_num = real(drugsubstance_str)
	
		label define drugsubstance_lb 1"agomelatine" 2"amitriptyline" 3"amoxapine" 4"butriptyline" 5"citalopram" 6"clomipramine" 7"desipramine" 8"dosulepin" 9"doxepin" 10"duloxetine" 11"escitalopram" 12"fluoxetine" 13"fluvoxamine" 14"imipramine" 15"iprindole" 16"iproniazide" 17"isocarboxazid" 18"lofepramine" 19"maprotiline" 20"mianserin" 21"mirtazapine" 22"moclobemide" 23"nefazodone" 24"nortriptyline" 25"paroxetine" 26"phenelzine" 27"protriptyline" 28"reboxetine" 29"sertraline" 30"tranylcypromine" 31"trazodone" 32"trimipramine" 33"tryptophan" 34"venlafaxine" 35"vortioxetine"
	
	label value drugsubstance_num drugsubstance_lb
	
	tab drugsubstance
	tab drugsubstance_num, nolabel
	
	drop drugsubstance_str drugsubstance
	rename drugsubstance_num drugsubstance
	
	tab drugsubstance
	tab drugsubstance, nolabel
	
	keep patid presc_startdate_num presc_enddate_num prodcode drugsubstance class qty clean_daily_dose dose_unit prescr_length2 dd_mg flag_imputed*
	order patid presc_startdate_num presc_enddate_num prodcode drugsubstance class qty clean_daily_dose dose_unit prescr_length2 dd_mg flag_imputed*
	
	label variable clean_daily_dose				"Daily dose"
	label variable prescr_length2				"Length of prescription in days"
	label variable flag_imputed_daily_dose 		"Imputed daily dose"
	label variable flag_imputed_qty 			"Imputed quantity"
	label variable flag_imputed_prescr_length 	"Imputed length of prescription (either daily dose or quantity)"
	label variable flag_imputed_dd_mg 			"Imputed daily dose in mg (daily dose imputed)"
	label variable presc_startdate_num 			"Prescription start date (numeric)"
	label variable presc_enddate_num 			"Prescription end date (numeric)"
	
	rename clean_daily_dose daily_dose
	rename prescr_length2 prescr_length
	
	compress
	duplicates drop		// 26,296 duplicates dropped
	count				// 3,893,328 (100%)
	
********************************************************************************

* Save this as a permanent dataset

	save "$Datadir\derived_data\AD_pxn_events_from_All_Therapy_clean.dta", replace
	
********************************************************************************
	
* Clear temporary datasets

	erase "$Tempdatadir\AD_pxn_events_from_All_Therapy_for_imputation.dta"
	erase "$Tempdatadir\AD_pxn_events_from_All_Therapy_imputed.dta"
	erase "$Tempdatadir\AD_pxn_events_from_All_Therapy_prescr_length.dta"
	erase "$Tempdatadir\AD_pxn_events_from_All_Therapy_dd_mg.dta"
	
********************************************************************************

* Stop logging

	log close
	
********************************************************************************
