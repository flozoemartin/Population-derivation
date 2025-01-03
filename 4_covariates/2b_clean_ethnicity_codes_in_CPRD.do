****************************************************************************

* Create ethnicity categories using CPRD Gold data and attach to the denominator file

* Author: Rohini Mathur & Harriet Forbes (adapted for antidepressant project by Flo Martin)

* Date: 26/09/2022

********************************************************************************

* Create file of all ethnicity codes in the Clinical file 

	use "$Datadir\formatted_cprd_data\Clinical_0.dta", clear 
	
	forvalues x=1/11 {
		
		append using "$Datadir\formatted_cprd_data\Clinical_`x'.dta"
		
	}
	
	keep patid medcode enttype adid eventdate_num sysdate_num
	merge m:1 medcode using "$Codesdir\codelist_ethnicity_gold.dta", keep(3) nogen 
	
	save "$Tempdatadir\interim_ethnicity_codes.dta", replace 

	* Drop  codes pertaining to New Zealand
	gen nzsubstr=substr(readcode,1,2)
	tab nzsubstr
	browse nz readterm if nzsubstr=="9T"
	tab readterm if nzsubstr=="9T" // inc. "ethnicity and other related nationalities", "indian", "nz ethnic group nos", "nz ethnic groups", "other asian", "south east asian" 
	drop if nzsubstr=="9T" & (regexm(readterm, "new zealand") | regexm(readterm, "ethnicity")) // 8,224 deleted (the nz ones and the generic ethnicity one) 

	* Drop true duplicates - n = 13,681
	duplicates drop

* Turn sysdate format into years - note system date more complete than eventdate

	gen sysyear=year(sysdate)
	sum sysyear // 1996-2021

* Tag people with the same ethnicity recorded in the same year

	duplicates tag patid sysyear readcode, gen(duplicate)
	tab duplicate if duplicate>0

	lab var duplicate "duplicate ethnicity recorded in the same year"

	* Generate indicators for observations of ethnicity per patient

	sort patid eventdate
	bysort patid: gen count=[_n]
	bysort patid: gen total=[_N]
	sum count total

	gen totalobs=total
	recode totalobs (2/5=2) (6/10=3) (11/max=3), gen(obsgroup)
	label define obsgroup 1"1" 2"2-5" 3"6-57"
	label values obsgroup obsgroup
	tab obsgroup

* Add up ethnicities

	bysort patid eth5: gen eth5count=[_N]
	bysort patid eth16: gen eth16count=[_N]
	tab eth5count
	tab eth16count

	gen white5count=eth5count if eth5==0
	gen sa5count=eth5count if eth5==1
	gen black5count=eth5count if eth5==2
	gen other5count=eth5count if eth5==3
	gen mixed5count=eth5count if eth5==4
	gen notstated5count=eth5count if eth5==5

	tab white5count if count==1
	tab white5count if count==total

	gen british16count=eth16count if eth16==1
	gen irish16count=eth16count if eth16==2
	gen otherwhite16count=eth16count if eth16==3
	gen whitecarib16count=eth16count if eth16==4
	gen whiteaf16count=eth16count if eth16==5
	gen whiteasian16count=eth16count if eth16==6
	gen othermixed16count=eth16count if eth16==7
	gen indian16count=eth16count if eth16==8
	gen pak16count=eth16count if eth16==9
	gen bangla16count=eth16count if eth16==10
	gen otherasian16count=eth16count if eth16==11
	gen carib16count=eth16count if eth16==12
	gen african16count=eth16count if eth16==13
	gen otherblack16count=eth16count if eth16==14
	gen chinese16count=eth16count if eth16==15
	gen other16count=eth16count if eth16==16
	gen notstated16count=eth16count if eth16==17

* Make counts constant

	local p  "white sa black other mixed notstated"
	foreach i of local p {

		sort patid count
		replace `i'5count=`i'5count[_n-1] if `i'5count[_n]==. & `i'5count[_n-1]!=. & patid[_n]==patid[_n-1] & totalobs>1
		gsort patid -count
		replace `i'5count=`i'5count[_n-1] if `i'5count[_n]==. & `i'5count[_n-1]!=. & patid[_n]==patid[_n-1] & totalobs>1

	}

	local p "british irish otherwhite whitecarib whiteaf whiteasian othermixed indian pak bangla otherasian carib african otherblack chinese other notstated"
	foreach i of local p {

		sort patid count
		replace `i'16count=`i'16count[_n-1] if `i'16count[_n]==. & `i'16count[_n-1]!=. & patid[_n]==patid[_n-1] & totalobs>1
		gsort patid -count
		replace `i'16count=`i'16count[_n-1] if `i'16count[_n]==. & `i'16count[_n-1]!=. & patid[_n]==patid[_n-1] & totalobs>1

	}

* Dummy for whether only ethnicity is not stated

	gen notstatedonly=0
	replace notstatedonly=1 if white5count==. & sa5count==. & black5count==. & other5count==. & mixed5count==. & notstated5count!=. 

* Make constant

	sort patid count
	replace notstatedonly=notstatedonly[_n-1] if notstatedonly[_n]==0 & notstatedonly[_n-1]!=0 & patid[_n]==patid[_n-1] & totalobs>1
	gsort patid -count
	replace notstatedonly=notstatedonly[_n-1] if notstatedonly[_n]==0 & notstatedonly[_n-1]!=0 & patid[_n]==patid[_n-1] & totalobs>1
	sort patid count

	gen enter=1 if count==1
	gen exit=1 if count==total
	tab enter exit
	tab notstatedonly if enter==1, missing
	tab notstatedonly if exit==1, missing

/*
notstatedon |
         ly |      Freq.     Percent        Cum.
------------+-----------------------------------
          0 |  1,882,386       94.45       94.45
          1 |    110,687        5.55      100.00
------------+-----------------------------------
      Total |  1,993,073      100.00
*/

* Most common ethnicity exlcuding "Not stated"

	egen eth5max=rowmax(white5count sa5count black5count other5count mixed5count)
	tab eth5max

	egen eth16max=rowmax(british16count irish16count otherwhite16count whitecarib16count whiteaf16count whiteasian16count othermixed16count indian16count pak16count bangla16count otherasian16count carib16count african16count otherblack16count chinese16count other16count) 
	tab eth16max

**ETH 5
	gen mostcommoneth5=eth5 if eth5max==totalobs // give most common eth a value if a person only had 1 ethnicity recorded in all observations
	replace mostcommoneth5=eth5 if totalobs==1   //makes mostcomoneth==eth if a person only has 1 observation
	replace mostcommoneth5=0 if eth5max==white5count & eth5max!=.
	replace mostcommoneth5=1 if eth5max==sa5count & eth5max!=.
	replace mostcommoneth5=2 if eth5max==black5count & eth5max!=.
	replace mostcommoneth5=3 if eth5max==other5count & eth5max!=.
	replace mostcommoneth5=4 if eth5max==mixed5count & eth5max!=.
	replace mostcommoneth5=5 if notstatedonly==1
	label values mostcommoneth5 eth5

	tab mostcommoneth5 if enter==1
	tab mostcommoneth5 if exit==1

/*
mostcommoneth5 |      Freq.     Percent        Cum.
---------------+-----------------------------------
      0. White |  1,655,374       83.06       83.06
1. South Asian |     94,351        4.73       87.79
      2. Black |     64,388        3.23       91.02
      3. Other |     39,589        1.99       93.01
      4. Mixed |     28,684        1.44       94.45
 5. Not Stated |    110,687        5.55      100.00
---------------+-----------------------------------
         Total |  1,993,073      100.00
*/

* People with two ethnicities that are equally most common - exclude "Not stated"
* This creates a dummy which is equal to 1 if 2 ethnicities are equally common, but only 1 has been coded as being "mostcommoneth5"
	
	gen equallycommon5=0
	replace equallycommon5=1 if eth5max==white5count & mostcommoneth!=0 & totalobs!=1 & notstatedonly==0
	replace equallycommon5=1 if eth5max==sa5count & mostcommoneth!=1 & totalobs!=1 & notstatedonly==0
	replace equallycommon5=1 if eth5max==black5count & mostcommoneth!=2 & totalobs!=1 & notstatedonly==0
	replace equallycommon5=1 if eth5max==other5count & mostcommoneth!=3 & totalobs!=1 & notstatedonly==0
	replace equallycommon5=1 if eth5max==mixed5count & mostcommoneth!=4 & totalobs!=1 & notstatedonly==0
	tab equallycommon5

/*
equallycomm |
        on5 |      Freq.     Percent        Cum.
------------+-----------------------------------
          0 |  2,501,929       98.97       98.97
          1 |     25,996        1.03      100.00
------------+-----------------------------------
      Total |  2,527,925      100.00
*/

* Update mostcommoneth to separate those with equally common ethnicities
	
	replace mostcommoneth5=6 if equallycommon5==1  & notstatedonly==0

* eth16
	gen  mostcommoneth16=eth16 if eth16max==totalobs // give most common eth a value if a person only had 1 ethnicity recorded in all observations
	replace mostcommoneth16=eth16 if totalobs==1   //makes mostcomoneth==eth if a person only has 1 observation
	replace mostcommoneth16=1 if eth16max==british16count & eth16max!=.
	replace mostcommoneth16=2 if eth16max==irish16count & eth16max!=.	
	replace mostcommoneth16=3 if eth16max==otherwhite16count & eth16max!=.
	replace mostcommoneth16=4 if eth16max==whitecarib16count & eth16max!=.
	replace mostcommoneth16=5 if eth16max==whiteaf16count & eth16max!=.
	replace mostcommoneth16=6 if eth16max==whiteasian16count & eth16max!=.
	replace mostcommoneth16=7 if eth16max==othermixed16count & eth16max!=.
	replace mostcommoneth16=8 if eth16max==indian16count & eth16max!=.
	replace mostcommoneth16=9 if eth16max==pak16count & eth16max!=.
	replace mostcommoneth16=10 if eth16max==bangla16count & eth16max!=.
	replace mostcommoneth16=11 if eth16max==otherasian16count & eth16max!=.
	replace mostcommoneth16=12 if eth16max==carib16count & eth16max!=.
	replace mostcommoneth16=13 if eth16max==african16count & eth16max!=.
	replace mostcommoneth16=14 if eth16max==otherblack16count & eth16max!=.
	replace mostcommoneth16=15 if eth16max==chinese16count & eth16max!=.
	replace mostcommoneth16=16 if eth16max==other16count & eth16max!=.
	replace mostcommoneth16=17 if notstatedonly==1
	label values mostcommoneth16 eth16

	tab mostcommoneth16 if enter==1
	tab mostcommoneth16 if exit==1

/*
             mostcommoneth16 |      Freq.     Percent        Cum.
-----------------------------+-----------------------------------
                  1. British |  1,513,870       75.96       75.96
                    2. Irish |     16,620        0.83       76.79
              3. Other White |    124,681        6.26       83.05
4. White and Black Caribbean |      5,474        0.27       83.32
  5. White and Black African |      3,894        0.20       83.52
          6. White and Asian |      4,642        0.23       83.75
              7. Other Mixed |     11,818        0.59       84.34
                   8. Indian |     36,091        1.81       86.15
                9. Pakistani |     24,794        1.24       87.40
             10. Bangladeshi |      8,332        0.42       87.81
             11. Other Asian |     25,752        1.29       89.11
               12. Caribbean |     10,815        0.54       89.65
                 13. African |     36,765        1.84       91.49
             14. Other Black |     18,191        0.91       92.41
                 15. Chinese |     10,985        0.55       92.96
      16. Other ethnic group |     29,662        1.49       94.45
              17. Not Stated |    110,687        5.55      100.00
-----------------------------+-----------------------------------
                       Total |  1,993,073      100.00
*/

* People with two ethnicities that are equally most common - exclude "Not stated"
* This creates a dummy which is equal to 1 if 2 ethnicities are equally common, but only 1 has been coded as being "mostcommoneth16"
	
	gen equallycommon16=0
	replace equallycommon16=1 if eth16max==british16count & mostcommoneth16!=1 & totalobs!=1 & notstatedonly==0
	replace equallycommon16=1 if eth16max==irish16count & mostcommoneth16!=2 & totalobs!=1 & notstatedonly==0
	replace equallycommon16=1 if eth16max==otherwhite16count & mostcommoneth16!=3 & totalobs!=1 & notstatedonly==0
	replace equallycommon16=1 if eth16max==whitecarib16count & mostcommoneth16!=4 & totalobs!=1 & notstatedonly==0
	replace equallycommon16=1 if eth16max==whiteaf16count & mostcommoneth16!=5 & totalobs!=1 & notstatedonly==0
	replace equallycommon16=1 if eth16max==whiteasian16count & mostcommoneth16!=6 & totalobs!=1 & notstatedonly==0
	replace equallycommon16=1 if eth16max==othermixed16count & mostcommoneth16!=7 & totalobs!=1 & notstatedonly==0
	replace equallycommon16=1 if eth16max==indian16count & mostcommoneth16!=8 & totalobs!=1 & notstatedonly==0
	replace equallycommon16=1 if eth16max==pak16count & mostcommoneth16!=9 & totalobs!=1 & notstatedonly==0
	replace equallycommon16=1 if eth16max==bangla16count & mostcommoneth16!=10 & totalobs!=1 & notstatedonly==0
	replace equallycommon16=1 if eth16max==otherasian16count & mostcommoneth16!=11 & totalobs!=1 & notstatedonly==0
	replace equallycommon16=1 if eth16max==carib16count & mostcommoneth16!=12 & totalobs!=1 & notstatedonly==0
	replace equallycommon16=1 if eth16max==african16count & mostcommoneth16!=13 & totalobs!=1 & notstatedonly==0
	replace equallycommon16=1 if eth16max==otherblack16count & mostcommoneth16!=14 & totalobs!=1 & notstatedonly==0
	replace equallycommon16=1 if eth16max==chinese16count & mostcommoneth16!=15 & totalobs!=1 & notstatedonly==0
	replace equallycommon16=1 if eth16max==other16count & mostcommoneth16!=16 & totalobs!=1 & notstatedonly==0
	tab equallycommon16

/*
equallycomm |
       on16 |      Freq.     Percent        Cum.
------------+-----------------------------------
          0 |  2,468,824       97.66       97.66
          1 |     59,101        2.34      100.00
------------+-----------------------------------
      Total |  2,527,925      100.00
*/

* Update mostcommoneth to separate those with equally common ethnicities

	replace mostcommoneth16=18 if equallycommon16==1 & notstatedonly==0

	label define eth5 6"equally common", add
	label define eth16 18"equally common", add
*bro patid count eth5 mostcommoneth5 notstatedonly if equallycommon5==1
	bro patid count eth16 mostcommoneth16 notstatedonly if equallycommon16==1
	compress

* Ethnicity
* Patients with valid ethnicity ever recorded

	gen anyethever=0
	replace anyethever=1 if eth16==.
	tab anyethever // 100% as these are only patients with ethncity codes - need to make this variable 0 when attached to denominator population

* Patients with valid ethnicity ever recorded
	
	gen validethever=0
	replace validethever=1 if eth16!=17
	replace validethever=0 if eth16==.
	tab validethever

/*
validetheve |
          r |      Freq.     Percent        Cum.
------------+-----------------------------------
          0 |    172,211        6.81        6.81
          1 |  2,355,714       93.19      100.00
------------+-----------------------------------
      Total |  2,527,925      100.00
*/

* Make ethever constant within patients
	
	sort patid count
	replace anyethever=anyethever[_n-1] if anyethever[_n]==0 & anyethever[_n-1]==1 & patid[_n]==patid[_n-1]
	replace validethever=validethever[_n-1] if validethever[_n]==0 & validethever[_n-1]==1 & patid[_n]==patid[_n-1]
	gsort patid -count
	replace anyethever=anyethever[_n-1] if anyethever[_n]==0 & anyethever[_n-1]==1 & patid[_n]==patid[_n-1]
	replace validethever=validethever[_n-1] if validethever[_n]==0 & validethever[_n-1]==1 & patid[_n]==patid[_n-1]
	sort patid count
	replace anyethever=0 if anyethever==.
	replace validethever=0 if validethever==.

* Count of valid ethnicities recorded

	gen validethcount=1 if eth16!=17 & eth16!=.
	replace validethcount=0 if eth16==17 | eth16==.

	sort patid count
	replace validethcount=validethcount[_n]+validethcount[_n-1] if patid[_n]==patid[_n-1]

* Total number of ethnicities recorded (including multiple recordings of the same ethnicity)

	gen totalvalideth=validethcount if exit==1
	tab totalvalideth

* Make totaleth constant for each patient

	sort patid count
	replace totalvalideth=totalvalideth[_n-1] if totalvalideth[_n]==. & totalvalideth[_n-1]!=. & patid[_n]==patid[_n-1]
	gsort patid -count
	replace totalvalideth=totalvalideth[_n-1] if totalvalideth[_n]==. & totalvalideth[_n-1]!=. & patid[_n]==patid[_n-1]
	sort patid count

* Dummy for multiple ethnicities excluding not stated
	
	gen morethanoneeth=0 if totalvalideth<=1
	replace morethanoneeth=1 if totalvalideth>1
	tab morethanoneeth

/*
morethanone |
        eth |      Freq.     Percent        Cum.
------------+-----------------------------------
          0 |  1,687,503       66.75       66.75
          1 |    840,422       33.25      100.00
------------+-----------------------------------
      Total |  2,527,925      100.00
*/

* Generate variable for the first year ethnicity was recorded using frd

	sort patid count
	gen firstyear=year(eventdate) if count==1
	destring firstyear, replace
	sum firstyear //1956-2021

	replace firstyear=firstyear[_n-1] if patid[_n]==patid[_n-1] & [_n]!=1 

* Are ethnicities matching?
* Do not give unknown ethnicity a unique counter

	sort patid eth16 count
	gen uniqueeth=0
	replace uniqueeth=1 if eth16[_n]!=eth16[_n-1] & patid[_n]==patid[_n-1] & eth16[_n]!=17 & eth16[_n-1]!=17
	replace uniqueeth = 1 if patid[_n]>patid[_n-1]
	replace uniqueeth=0 if eth16==17
	tab uniqueeth

/* 
  uniqueeth |      Freq.     Percent        Cum.
------------+-----------------------------------
          0 |    603,618       23.88       23.88
          1 |  1,924,307       76.12      100.00
------------+-----------------------------------
      Total |  2,527,925      100.00
*/

* Count unique ethnicities and exclude unknown
	
	sort patid count
	replace uniqueeth=uniqueeth[_n]+uniqueeth[_n-1] if patid[_n]==patid[_n-1] & uniqueeth[_n]!=. & uniqueeth[_n-1]!=. & count!=1 
	gsort patid -count
	sum uniqueeth // people have up to a maximum of 13 different ethnicities

	sort patid eth16 uniqueeth
	replace uniqueeth=uniqueeth[_n-1] if uniqueeth[_n]==. & uniqueeth[_n-1]!=. & patid[_n]==patid[_n-1]

* ethsum gives the number of different ethnic groups recorded
* totaluniqueeth gives the number of ethnicities recorded per patient - excluding duplicates

* Count of unique ethnicities per patient

	sort patid uniqueeth
	bysort patid: gen totaluniqueeth = uniqueeth[_N] 
	tab totaluniqueeth if enter==1
	tab totaluniqueeth if exit==1

* Dummy for yes no to having multiple unique ethnicities
* Unknown ethnicity is excluded from all counts

	gen sameeth=1 if totaluniqueeth==1
	replace sameeth=0 if totaluniqueeth>1
	tab sameeth if enter==1
	tab sameeth if exit==1

/*  
    sameeth |      Freq.     Percent        Cum.
------------+-----------------------------------
          0 |     39,527        2.10        2.10
          1 |  1,842,858       97.90      100.00
------------+-----------------------------------
      Total |  1,882,385      100.00
*/

* Indicator for whether all of the ethnicities fall under the same high level group
* Ethnicity which is unknown is ignored
* i.e., if a person has 3 ethnicities, white, british and unknown, then they are considered to have matching eth5

	sort patid count
	gen eth5same=1 if enter==1
	replace eth5same=1 if eth5[_n]==eth5[_n-1] & patid[_n]==patid[_n-1]
	replace eth5same=1 if eth5==5
	tab eth5same, missing

* If any eth5same values are missing - then replace as 0 and make constant

	replace eth5same=0 if eth5same==.
	replace eth5same=eth5same[_n-1] if eth5same[_n-1]==0 & patid[_n]==patid[_n-1]
	gsort patid -count
	replace eth5same=eth5same[_n-1] if eth5same[_n-1]==0 & patid[_n]==patid[_n-1]
	sort patid count

	tab eth5same if enter==1
	tab eth5same if exit==1

	sort patid count
	gen eth16same=1 if enter==1
	replace eth16same=1 if eth16[_n]==eth16[_n-1] & patid[_n]==patid[_n-1]
	replace eth16same=1 if eth16==17
	tab eth16same, missing

* If any eth5same values are missing - then replace as 0 and make constant
	
	replace eth16same=0 if eth16same==.
	replace eth16same=eth16same[_n-1] if eth16same[_n-1]==0 & patid[_n]==patid[_n-1]
	gsort patid -count
	replace eth16same=eth16same[_n-1] if eth16same[_n-1]==0 & patid[_n]==patid[_n-1]
	sort patid count

	tab eth16same if enter==1
	tab eth16same if exit==1

* Fixed variable for latest ethnicity

	gsort patid -eventdate
	gen latesteth=readcode 
	gen latestdesc=readterm 
	gen latesteth16=eth16 
	gen latesteth5=eth5

	replace latesteth=latesteth[_n-1] if patid[_n]==patid[_n-1] & [_n]!=1 
	replace latestdesc=latestdesc[_n-1] if patid[_n]==patid[_n-1] & [_n]!=1 
	replace latesteth16=latesteth16[_n-1] if patid[_n]==patid[_n-1] & [_n]!=1 
	replace latesteth5=latesteth5[_n-1] if patid[_n]==patid[_n-1] & [_n]!=1 

	label values latesteth16 eth16
	label values latesteth5 eth5

	save "$Tempdatadir\All Ethnicity GOLD CPRD.dta", replace


* Make patient-level file and drop duplicate patids

	use "$Datadir\formatted_cprd_data\All_Patient.dta", clear

	merge 1:m patid using "$Tempdatadir\All Ethnicity GOLD CPRD.dta", gen(merge2)

/*     
    Result                      Number of obs
    -----------------------------------------
    Not matched                     2,729,152
        from master                 2,729,049  (merge2==1)
        from using                        103  (merge2==2)

    Matched                         2,527,822  (merge2==3)
    -----------------------------------------
*/

	codebook patid // 4,722,122 
	duplicates drop patid, force // n = 534,852 obs deleted
	codebook patid // 4,722,122

	keep patid accept latesteth16 latesteth5 mostcommon*

* Merge with HES ethncity
	merge 1:1 patid using "$Tempdatadir\ethnicity_hes.dta", nogen
	codebook patid // 4,722,146 -  afew people in HES are not in CPRD - keep for now. TO CHECK

* Generate one variable for ethnicity
 
	gen eth5=mostcommoneth5 // main ethnicity is most common in CPRD
	label values eth5 eth5
	tab eth5, missing

/*  
          eth5 |      Freq.     Percent        Cum.
---------------+-----------------------------------
      0. White |  1,655,374       35.06       35.06
1. South Asian |     93,337        1.98       37.03
      2. Black |     63,057        1.34       38.37
      3. Other |     34,585        0.73       39.10
      4. Mixed |     23,958        0.51       39.61
 5. Not Stated |    110,687        2.34       41.95
equally common |     12,075        0.26       42.21
             . |  2,729,073       57.79      100.00
---------------+-----------------------------------
         Total |  4,722,146      100.00
*/

* Remove equally common group

	replace eth5=latesteth5 if eth5>=5 & latesteth5!=.  // replace ethnicity with latest eth5 if mostcommoneth5 is not stated/equal/missing
	tab eth5, missing

/*
          eth5 |      Freq.     Percent        Cum.
---------------+-----------------------------------
      0. White |  1,658,845       35.13       35.13
1. South Asian |     95,141        2.01       37.14
      2. Black |     64,541        1.37       38.51
      3. Other |     37,365        0.79       39.30
      4. Mixed |     26,366        0.56       39.86
 5. Not Stated |    110,815        2.35       42.21
             . |  2,729,073       57.79      100.00
---------------+-----------------------------------
         Total |  4,722,146      100.00
*/

* Add HES ethnicity where missing in CPRD
	
	replace eth5=heseth5 if eth5>4 & heseth5!=. //replace ethnicity with HES ethnicity if still missing/notstated/equal
	tab eth5, missing

/* 
          eth5 |      Freq.     Percent        Cum.
---------------+-----------------------------------
      0. White |  2,595,936       54.97       54.97
1. South Asian |    132,932        2.82       57.79
      2. Black |     88,436        1.87       59.66
      3. Other |     58,022        1.23       60.89
      4. Mixed |     45,010        0.95       61.84
 5. Not Stated |    116,392        2.46       64.31
             . |  1,685,418       35.69      100.00
---------------+-----------------------------------
         Total |  4,722,146      100.00
*/

	gen eth16=mostcommoneth16 
	label values eth16 eth16
	replace eth16=latesteth16 if eth16>=17 & latesteth16<17  // replace ethnicity with latest eth16 if mostcommoneth5 is not stated/equal/missing
	tab eth16, missing
	replace eth16=heseth16 if eth16>=17 & heseth16!=. // the standard HES categories don't map to the 16 census categories (ie/ HES has White not split into British/Irish/Other White and Mixed not split into Mixed White/African White/Caribbean etc..)
	tab eth16 eth5, missing
	tab eth16, m

	label define eth16 17"Unknown" 18"White (HES)" 19"Mixed (HES)", modify

/* 					 
                       eth16 |      Freq.     Percent        Cum.
-----------------------------+-----------------------------------
                  1. British |  1,520,477       32.20       32.20
                    2. Irish |     16,063        0.34       32.54
              3. Other White |    122,213        2.59       35.13
4. White and Black Caribbean |      5,653        0.12       35.25
  5. White and Black African |      4,262        0.09       35.34
          6. White and Asian |      4,781        0.10       35.44
              7. Other Mixed |     11,700        0.25       35.69
                   8. Indian |     50,878        1.08       36.76
                9. Pakistani |     35,484        0.75       37.51
             10. Bangladeshi |     11,701        0.25       37.76
             11. Other Asian |     34,820        0.74       38.50
               12. Caribbean |     17,575        0.37       38.87
                 13. African |     49,822        1.06       39.93
             14. Other Black |     20,980        0.44       40.37
                 15. Chinese |     14,659        0.31       40.68
      16. Other ethnic group |     43,450        0.92       41.60
              17. Not Stated |    116,344        2.46       44.07
              equally common |    937,217       19.85       63.91
                          19 |     18,649        0.39       64.31
                           . |  1,685,418       35.69      100.00
-----------------------------+-----------------------------------
                       Total |  4,722,146      100.00
*/

	compress
	notes: This dataset includes some people in HES who are not in Jul 2021 CPRD
	notes

* Dummy for whether ethnicity is derived from CPRD or HES

	gen ethnicity_source=0
	replace ethnicity_source=1 if eth5==heseth5 & mostcommoneth5==. & latesteth5==.
	label define ethnicity_source 0"CPRD" 1"HES"
	label values ethnicity_source ethnicity_source
	replace ethnicity_source=. if eth5==.
	tab ethnicity_source, m

* Save "$Tempdatadir\ethnicity_final.dta" for final cleaning in main covariate derivation script

	keep patid accept eth5 eth16 ethnicity_source heseth5
	replace eth5=heseth5 if eth5>4 & heseth5!=. // replace ethnicity with HES ethnicity if missing - 0 changes made
	drop heseth5
	merge 1:m patid using "$Datadir\derived_data\pregnancy_cohort_final.dta", keep(3) nogen
	keep patid pregid eth5
	save "$Tempdatadir\ethnicity_final.dta", replace

* Check: expect 99% to have ethnicity among those with live birth 2010-2016, plus eligible for HES linkage, with 85% White ethnicity
	
	use "$Tempdatadir\ethnicity_final.dta", clear
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_final.dta"
	merge 1:1 patid pregid using "$Datadir\derived_data\pregnancy_cohort_conflicts_outcome_update.dta", keep(match) nogen
	merge m:1 patid using "$Datadir\formatted_linked_data\linkage_eligibility.dta", keep(match) nogen keepusing(hes_apc_e)
	tab hes_apc_e
	tab updated_outcome
	gen pregyear=year(pregstart_num)
	tab eth5 if pregyear>=2010 & pregyear<=2016 & outcome==1 & hes_apc_e==1, miss // n = 26

********************************************************************************

* Erase unnecessary datasets
	
	erase "$Tempdatadir\All Ethnicity GOLD CPRD.dta"
	erase "$Tempdatadir\interim_ethnicity_codes.dta"
	
********************************************************************************
