********************************************************************************

* Program to pull out BMI status for each CPRD patient

* Author: Paul Madley-Dowd (adapted by Flo Martin)

* Date: 14/10/2022

********************************************************************************

	cap prog drop pr_getbmistatus
	program define pr_getbmistatus

		syntax, index(string) patientfile(string) clinicalfile(string) additionalfile(string)

		noi di
		noi di in yellow _dup(5) "*"
		noi di in yellow "Assign BMI:"
		noi di in yellow "based on nearest status pre index date:"

		noi di in yellow _dup(5) "*"

		preserve
		noi pr_getallbmirecords, patientfile("`patientfile'") clinicalfile("`clinicalfile'") additionalfile("`additionalfile'")
		tempfile allbmirecords
		save `allbmirecords'
		restore

		qui{

			merge 1:m patid using `allbmirecords'

			sort patid dobmi

			* Algorithm: (index in pregstart)
			
			rename dobmi eventdate_num
			gen _distance = eventdate_num-`index'
			drop if eventdate_num>=secondtrim_num & eventdate_num<=pregend+60
			gen _priority = 1 if _distance>=0 & eventdate_num<secondtrim_num // first trimester
			replace _priority = 2 if _distance<0 & _distance>=-(365*5) // within 5 years before pregnancy
			replace _priority = 3 if eventdate_num>=pregend_num+60 & eventdate_num<=pregend_num+365.25 // year after pregnancy
			replace _priority = 4 if _distance<(365.25*10) & _distance>-(365*10) // within 10 years of pregnancy either side
			replace _priority = 5 if eventdate_num>=pregend_num+(365.25*10) // more than 10 years after pregnancy

			gen _absdistance = abs(_distance)
			sort patid _priority _absdistance
			by patid: replace bmi = bmi[1] 
			drop eventdate_num _merge _distance _priority _absdistance

			by patid: keep if _n==1

		}/*end of quietly*/

	end
	
********************************************************************************
