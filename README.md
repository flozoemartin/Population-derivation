# Population-derivation

The derivation of the underlying CPRD GOLD population used in the studies for my PhD thesis titled "Antidepressant use during pregnancy: what are the outcomes?". 

The scripts are organised as follows:

- **1_formatting**: the conversion of text files as received from CPRD into .dta files for use in Stata, labelling variables as per the documentation, and merging datasets across data deliveries.
- **2_eligibility**: retrieving information on unknown outcomes from HES, resolving conflicting and historical pregnancies, defining gravidity and parity, identifying birthweight from HES Maternity, applying other broad eligibility criteria (1996-2018, follow-up, etc.)
- **3_exposure**: identifying and cleaning antidepressant prescriptions, applying hot decking imputation approach to fill in missing daily dose and quantity
- **4_covariates**: identifying and cleaning covariate variables e.g., BMI, smoking, alcohol, other prescriptions
- **5_indications**: identifying and cleaning indications e.g., depression, anxiety, other somatic indications

Subsequent projects where this population was used:

- **Patterns of antidepressant prescribing in and around pregnancy: a descriptive analysis in the UK Clinical Practice Research Datalink**
  - GitHub repository containing project scripts: https://github.com/flozoemartin/Patterns
  - medRxiv pre-print: https://www.medrxiv.org/content/10.1101/2024.08.08.24311553v1
- **First trimester antidepressant use and miscarriage: a comprehensive analysis in the UK Clinical Practice Research Datalink**
  - GitHub repository containing project scripts: https://github.com/flozoemartin/Miscarriage
  - medRxiv pre-print: https://www.medrxiv.org/content/10.1101/2024.10.19.24315779v1
- **Antidepressant use during pregnancy and birth outcomes: analysis of electronic health data from the UK, Norway, and Sweden**
  - GitHub repository containing project scripts: https://github.com/flozoemartin/Birth-outcomes
  - medRxiv pre-print: https://www.medrxiv.org/content/10.1101/2024.10.30.24316340v1
