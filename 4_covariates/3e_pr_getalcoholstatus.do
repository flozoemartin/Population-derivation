********************************************************************************

* Program to pull out alcohol status for each CPRD patient

* Author: Paul Madley-Dowd (adapted by Flo Martin)

* Date: 14/10/2022

********************************************************************************

	cap prog drop pr_getalcoholstatus
	program define pr_getalcoholstatus

		syntax, clinicalfile(string) additionalfile(string) alcoholcodelist(string) alcoholstatusvar(string) alcohollevelvar(string) index(string)

		noi di
		noi di in yellow _dup(5) "*"
		noi di in yellow "Assign alcohol status/level (from either clinical, therapy or additional file),"
		noi di in yellow "based on nearest status pre index date:"
		noi di in yellow _dup(5) "*"

* Pick up alcohol from Additional file for later
		
			preserve
			merge 1:m patid using "`additionalfile'", keep(match master) nogen
			keep if enttype==5
			destring data1, replace
			destring data2, replace
			rename data1 status
			rename data2 unitsperwk
			drop data*
			keep patid adid status unitsperwk pregid
			drop if (status==0|status==.) & unitsperwk==.
			recode status 1=1 2=0 3=2 /*to match the codes in the clinical codelist - i.e. 0=No 1=Yes 2=Ex*/
			gen unitscat=unitsperwk
			recode unitscat 0=0 1/14=1 15/42=2 43/10000=3
			drop unitsperwk
			tempfile additionalalcoholdata
			save `additionalalcoholdata'
			restore 

* Get alcohol treatment prescription codes and save for later

			preserve
			merge 1:m patid using "$Datadir\formatted_cprd_data\All_Therapy_reduced.dta", keep(match master) nogen
			merge m:1 prodcode using "$Codesdir\treatment_alc_abuse.dta", keep(match)
			rename _merge alcoholdatamatched
			bysort patid: keep if _n==1
			tempfile therapyalcoholdata
			save `therapyalcoholdata'
			restore 
	
* Get alcohol status from codes and supplement with info from Additional retrieved above

			merge 1:m patid using "`clinicalfile'", keep(match master) keepusing(eventdate_num medcode adid) nogen

			* rename eventdate_num eventdate_num_clin // otherwise end up with two eventdate_num's from Clinical and Therapy
			
			merge m:1 medcode using "`alcoholcodelist'", keepusing(`alcoholstatusvar' `alcohollevelvar') keep(match master)
			drop medcode
			rename _merge alcoholdatamatched

			replace adid = -_n if adid==0 /*this is just to avoid the "patid adid do not uniquely identify observations in the master data" error which is caused by all the adid=0s */
			merge 1:1 patid adid using `additionalalcoholdata'
			
			
			replace alcoholdatamatched=3 if _merge==3
			drop adid
			drop _merge
			merge m:1 patid using `therapyalcoholdata'
			
			*rename eventdate_num eventdate_num_ther
			replace alcoholdatamatched=3 if _merge==3

			drop _merge
			replace `alcoholstatusvar'=status if `alcoholstatusvar'==.
			replace `alcohollevelvar'=unitscat if `alcohollevelvar'==.
			drop status unitscat
			gsort patid -alcoholdatamatched eventdate
			by patid: drop if alcoholdatamatched !=3 & _n>1
			replace eventdate_num = date(eventdate, "DMY") if eventdate_num==.
			format eventdate_num %td
			replace eventdate_num =. if alcoholdatamatched!=3

* Assign status based on index date, using algorithm below
* Algorithm:
* Take the nearest status of -1y to +1month from index (best)...
* ...then nearest up to 1y after (second best)*...
* ...then any before (third best)...
* ...then any after (least best)

			gen _distance = eventdate_num - `index'
			drop if _distance>gestdays
			gen _priority = 1 if _distance>=0 
			replace _priority = 2 if _distance<0 & _distance>=-(365*10)

			gen _absdistance = abs(_distance)
			sort patid _priority _absdistance 

			if _N>0 {

* Patients nearest status is non-drinker, but have history of drinking, recode to ex-drinker

				by patid: egen ever_alc=sum(`alcoholstatusvar') 
				by patid: replace  `alcoholstatusvar' = 2 if ever_alc>0 & `alcoholstatusvar'==0

				sort patid _priority _absdistance
				by patid: replace `alcoholstatusvar' = `alcoholstatusvar'[1] 
				by patid: replace `alcohollevelvar' = `alcohollevelvar'[1] 
				drop alcoholdatamatched
				by patid: keep if _n==1
				
			}

			else if _N==0 {	// populate variables even if no data exists 
			
				keep patid pregid 
			
			}

	end
	
********************************************************************************
