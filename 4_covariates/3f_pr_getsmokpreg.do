********************************************************************************

* Program to pull out smoking for each CPRD patient

* Author: Paul Madley-Dowd (adapted by Flo Martin)

* Date: 14/10/2022

********************************************************************************

* pr_getsmokingstatus
* Adds smoking status to a file containing patid and an index date
	
	cap program drop pr_getsmok
	program define pr_getsmok

	syntax, clinicalfile(string) additionalfile(string) smokingcodelist(string) smokingstatusvar(string) index(string)

	noi di
	noi di in yellow _dup(5) "*"
	noi di in yellow "Assign smoking status (from either clinical codes, or additional file),"
	noi di in yellow "based on nearest status pre index date:"
	noi di in yellow _dup(5) "*"
	noi di in yellow "*HERE"

* Pick up smoking from Additional file for later
	
	preserve
	merge 1:m patid using "`additionalfile'", keep(match master) nogen
	keep if enttype==4 // enttype 4 relates to smoking
	desc
	destring data1, replace
	rename data1 status
	rename data2 cigsperday
	drop data*
	keep patid pregid adid status 
	drop if status==.|status==0
	recode status 1=1 2=0 3=2 /*to match clinical codelist*/
	label define statuslab 0 No 1 Yes 2 Ex
	label values status statuslab
	tempfile additionalsmokingdata
	save `additionalsmokingdata'
	restore 

* Get smoking status from codes, and supplement with info from Additional retrieved above

	merge 1:m patid using "`clinicalfile'", keep(match master) keepusing(eventdate_num medcode adid) nogen
	merge m:1 medcode using "`smokingcodelist'", keepusing(`smokingstatusvar') keep(match master)
	drop medcode
	rename _merge smokingdatamatched
	
	keep if eventdate_num>=pregstart_num & eventdate_num<pregend_num

	replace adid = -_n if adid==0 /*this is just to avoid the "patid adid do not uniquely identify observations in the master data" error which is caused by all the adid=0s*/
	merge 1:1 patid adid using `additionalsmokingdata'
	drop adid

	replace smokingdatamatched=3 if _merge==3
	drop _merge
	replace `smokingstatusvar'=status if `smokingstatusvar'==.

	drop status 

	gsort patid -smokingdatamatched eventdate_num
	by patid: drop if smokingdatamatched!=3 & _n>1
	replace eventdate_num=. if smokingdatamatched!=3

* Assign status based on index date, using algorithm below
* Algorithm:
* Take the smoking record during pregnancy (best)
* if not, then take any nearest before -10y from index if available (second best)

	gen _distance = eventdate_num-pregstart_num
	*drop if _distance>gestdays
	gen _priority = 1 if eventdate_num>=pregstart_num & eventdate_num<pregend_num
	*replace _priority = 2 if _distance<0 & _distance>=-(365*10)
	di "***HERE"
	gen _absdistance = abs(_distance)
	gen _nonspecific = (`smokingstatusvar'==12)

	sort patid _priority _absdistance _nonspecific

* Patients nearest status is non-smoker, but have history of smoking, recode to ex-smoker
	
	by patid: gen b4=1 if eventdate_num<=eventdate_num[1]
	drop if b4==.
	by patid: egen ever_smok=sum(`smokingstatusvar') 
	by patid: replace `smokingstatusvar' = 2 if ever_smok>0 & `smokingstatusvar'==0

	sort patid _priority _absdistance _nonspecific
	by patid: replace `smokingstatusvar' = `smokingstatusvar'[1] 
	drop smokingdatamatched _distance _priority _absdistance _nonspecific  
	by patid: keep if _n==1
	
	tab `smokingstatusvar', nol m
	
	recode `smokingstatusvar' .=0 // changing missing to non-smoker (assumption)
	recode `smokingstatusvar' 12=2 // changing uncertains to smoker (assumption)
	
	tab `smokingstatusvar'

	end
	
********************************************************************************
