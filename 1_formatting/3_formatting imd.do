********************************************************************************

* Format linked Index of Multiple Deprivation (IMD) data

* Author: Flo Martin (adapted from Harriet Forbes)

* Date: 12/05/2023

********************************************************************************

* Datasets generated by this do-file

*	- $Datadir\imd_practice.dta - formatted IMD practice dataset

********************************************************************************

* Start logging

	log using "$Logdir\1_formatting\3_formatting imd.txt", replace
	
********************************************************************************

	use "$Rawdatadir\original sept 21\linkage\stata\imd.dta", clear
	append using "$Rawdatadir\redelivery may 23\linkage\stata\imd_23.dta"
	
	duplicates drop
	
	count
	
	gen imd_practice = .
	
	foreach var in e2015_imd_5 ni2017_imd_5 s2016_imd_5 w2014_imd_5 {
	
		replace imd_practice = `var' if `var' !=.

	}
	
	* Label variables

	label variable pracid "Practice ID"
	label variable country "1=England, 2=Northern Ireland, 3=Scotland, 4=Wales"
	label variable e2015_imd_5 "England: IMD2015: quintile (1=LEAST deprived)"
	label variable ni2017_imd_5 "Northern Ireland: MDM2017: quintile (1=LEAST deprived)"
	label variable s2016_imd_5 "Scotland: IMD2016: quintile (1=LEAST deprived)"
	label variable w2014_imd_5 "Wales: IMD2014: quintile (1=LEAST deprived)"
	label variable imd_practice "IMD quintile for practice (1=LEAST deprived)"
	
	compress
	save "$Datadir\formatted_linked_data\imd_practice.dta", replace
	
********************************************************************************

* Stop logging
		
	log close
	
********************************************************************************
