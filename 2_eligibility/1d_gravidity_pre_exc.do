********************************************************************************

* Generating a gravidity variable having removed conflicting and historical pregnancies

* Author: Flo Martin

* Date: 03/11/2022

********************************************************************************

* Datasets generated by this do-file

* 		- $Datadir\covariates\new_gravhist.dta

********************************************************************************

* Gravidity number as a categorical variable - pregnum_new is gravidity 

	use "$Datadir\derived_data\pregreg_conflict_hist_rm.dta", clear
	keep patid pregid pregnum_new
	
	recode pregnum_new (1 = 0) (2 = 1) (3 = 2) (4/100 = 3), gen(gravidity_cat)
	label variable gravidity_cat "Gravidity"
	label define lb_graviditycat 0 "0" 1 "1" 2 "2" 3 "3+"
	label values gravidity_cat lb_graviditycat
	
	tab gravidity_cat
	
	save "$Datadir\covariates\new_gravidity.dta", replace
	
* Gravidity history - using all the information we have in the pregnancy register (after conflicting & historical algorithms but before eligibility criteria)

	use "$Datadir\derived_data\pregreg_conflict_hist_rm.dta", clear
	keep patid pregid pregnum_new updated_outcome 
	
	* Create index variable for reshaping
					
	sort patid pregnum_new
	summ pregnum_new
	local _presseqmax = r(max)
	
	reshape wide pregid updated_outcome, i(patid) j(pregnum_new)
	
	gen pregid23 =.
	gen updated_outcome23 =.
	gen grav_hist_sa1 =.
	gen grav_hist_sb1 =.
	gen grav_hist_top1 =.
	gen grav_hist_otherloss1 =.
	
	forvalues x=2/23 {
		
		local y=`x'-1
	
		gen grav_hist_sa`x' = 1 if updated_outcome`y'==4
		gen grav_hist_sb`x' = 1 if updated_outcome`y'==2 | updated_outcome`y'==3
		gen grav_hist_top`x' = 1 if updated_outcome`y'==5 | updated_outcome`y'==6
		gen grav_hist_otherloss`x' = 1 if updated_outcome`y'==7 | updated_outcome`y'==8 | updated_outcome`y'==9
		
	}
	
	forvalues x=2/23 {
		
		local y=`x'-1
	
		replace grav_hist_sa`x' = 1 if grav_hist_sa`y'==1
		replace grav_hist_sb`x' = 1 if grav_hist_sb`y'==1
		replace grav_hist_top`x' = 1 if grav_hist_top`y'==1
		replace grav_hist_otherloss`x' = 1 if grav_hist_otherloss`y'==1
		
	}
	
	reshape long pregid updated_outcome grav_hist_sa grav_hist_sb grav_hist_top grav_hist_otherloss, i(patid) j(pregnum_new)
	
	drop if pregid==.
	
	/*foreach x in sa sb top otherloss {
		
		rename grav_hist_`x'1 grav_hist_`x'_2
		rename grav_hist_`x'2 grav_hist_`x'_3
		rename grav_hist_`x'3 grav_hist_`x'_4
		rename grav_hist_`x'4 grav_hist_`x'_5
		rename grav_hist_`x'5 grav_hist_`x'_6
		rename grav_hist_`x'6 grav_hist_`x'_7
		rename grav_hist_`x'7 grav_hist_`x'_8
		rename grav_hist_`x'8 grav_hist_`x'_9
		rename grav_hist_`x'9 grav_hist_`x'_10
		rename grav_hist_`x'10 grav_hist_`x'_11
		rename grav_hist_`x'11 grav_hist_`x'_12
		rename grav_hist_`x'12 grav_hist_`x'_13
		rename grav_hist_`x'13 grav_hist_`x'_14
		rename grav_hist_`x'14 grav_hist_`x'_15
		rename grav_hist_`x'15 grav_hist_`x'_16
		rename grav_hist_`x'16 grav_hist_`x'_17
		rename grav_hist_`x'17 grav_hist_`x'_18
		rename grav_hist_`x'18 grav_hist_`x'_19
		rename grav_hist_`x'19 grav_hist_`x'_20
		rename grav_hist_`x'20 grav_hist_`x'_21
		rename grav_hist_`x'21 grav_hist_`x'_22
			
	}
	

	foreach y in `y' sa sb top otherloss {
		foreach x in 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 {
	
			replace grav_hist_`y'_`x' = 1 if grav_hist_`y'_2==1
		
		}
	
		foreach x in 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 {
	
			replace grav_hist_`y'_`x' = 1 if grav_hist_`y'_3==1
	
		}
	
		foreach x in 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 {
	
			replace grav_hist_`y'_`x' = 1 if grav_hist_`y'_4==1
		
		}
	
		foreach x in 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 {
	
			replace grav_hist_`y'_`x' = 1 if grav_hist_`y'_5==1
		
		}
	
		foreach x in 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 {
	
			replace grav_hist_`y'_`x' = 1 if grav_hist_`y'_6==1
		
		}
		
		foreach x in 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 {
	
			replace grav_hist_`y'_`x' = 1 if grav_hist_`y'_7==1
		
		}
	
		foreach x in 9 10 11 12 13 14 15 16 17 18 19 20 21 22 {
	
			replace grav_hist_`y'_`x' = 1 if grav_hist_`y'_8==1
		
		}
	
		foreach x in 10 11 12 13 14 15 16 17 18 19 20 21 22 {
	
			replace grav_hist_`y'_`x' = 1 if grav_hist_`y'_9==1
		
		}
	
		foreach x in 11 12 13 14 15 16 17 18 19 20 21 22 {
	
			replace grav_hist_`y'_`x' = 1 if grav_hist_`y'_10==1
		
		}

		foreach x in 12 13 14 15 16 17 18 19 20 21 22 {
	
			replace grav_hist_`y'_`x' = 1 if grav_hist_`y'_11==1
		
		}
	
		foreach x in 13 14 15 16 17 18 19 20 21 22 {
	
			replace grav_hist_`y'_`x' = 1 if grav_hist_`y'_12==1
		
		}
	
		foreach x in 14 15 16 17 18 19 20 21 22 {
	
			replace grav_hist_`y'_`x' = 1 if grav_hist_`y'_13==1
		
		}
	
		foreach x in 15 16 17 18 19 20 21 22 {
	
			replace grav_hist_`y'_`x' = 1 if grav_hist_`y'_14==1
		
		}
	
		foreach x in 16 17 18 19 20 21 22 {
	
			replace grav_hist_`y'_`x' = 1 if grav_hist_`y'_15==1
		
		}
	
		foreach x in 17 18 19 20 21 22 {
	
			replace grav_hist_`y'_`x' = 1 if grav_hist_`y'_16==1
		
		}
	
		foreach x in 18 19 20 21 22 {
	
			replace grav_hist_`y'_`x' = 1 if grav_hist_`y'_17==1
		
		}
	
		foreach x in 19 20 21 22 {
	
			replace grav_hist_`y'_`x' = 1 if grav_hist_`y'_18==1
		
		}
	
		foreach x in 20 21 22 {
	
			replace grav_hist_`y'_`x' = 1 if grav_hist_`y'_19==1
		
		}
		
		foreach x in 21 22 {
	
			replace grav_hist_`y'_21 = 1 if grav_hist_`y'_20==1
		
		}
	
		replace grav_hist_`y'_22 = 1 if grav_hist_`y'_21==1
	
	}
	
	reshape long pregid pregnum_new updated_outcome grav_hist_sa_ grav_hist_sb_ grav_hist_top_ grav_hist_otherloss_, i(patid) j(_presseq)
	count
	
	drop if pregid==. & updated_outcome==.
	drop _presseq*/
	count
	
	tab grav_hist_sa
	replace grav_hist_sa = 0 if grav_hist_sa!=1
	tab grav_hist_sa
	
	tab grav_hist_sb
	replace grav_hist_sb = 0 if grav_hist_sb!=1
	tab grav_hist_sb
	
	tab grav_hist_top
	replace grav_hist_top = 0 if grav_hist_top!=1
	tab grav_hist_top
	
	tab grav_hist_otherloss
	replace grav_hist_otherloss = 0 if grav_hist_otherloss!=1
	tab grav_hist_otherloss
	
	save "$Datadir\covariates\new_gravhist.dta", replace
	
********************************************************************************
