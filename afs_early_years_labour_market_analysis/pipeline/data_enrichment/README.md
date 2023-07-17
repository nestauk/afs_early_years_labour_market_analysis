# Data Enrichment

This folder contains the scripts used to enrich the OJO data used in the analysis.

## Enrichment steps

We enrich OJO data by adding:

- locations;
- salaries;
- description text;
- minimum early year practitioners qualification level;
- location urban/rural classification;

`enrich_relevant_jobs.py` - adds enrichment information. Also adds a dataset of skills extracted from the relevant job adverts. To run, execute the following command from this directory:

```bash
python enrich_relevant_jobs.py run
```

### Qualification level

We take a pattern matching approach to extracting the **minimum** qualification mentioned for a given job advert. The steps are as follows:

1. **Identify spans** that comply with a series of pattern based rules;
2. **Convert qualifications** that are not numbered (i.e. degree levels) to numbers as described in `level_dict`
3. **Extract numbers** from flagged, converted spans;
4. **Pick the minimum** qualification level number.

To evaluate this rules based approach, we label 100 randomly sampled (random_seed=42) EYP job adverts with qualifications extracted. The overall accuracy is **0.96**.

|              | precision | recall   | f1-score | support |
| ------------ | --------- | -------- | -------- | ------- |
| level 0      | 1         | 0.85     | 0.918919 | 20      |
| level 1      | 1         | 1        | 1        | 1       |
| level 2      | 0.956522  | 0.956522 | 0.956522 | 23      |
| level 3      | 0.955556  | 1        | 0.977273 | 43      |
| level 4      | 0         | 0        | 0        | 0       |
| level 6      | 1         | 1        | 1        | 13      |
| accuracy     | 0.96      | 0.96     | 0.96     | 0.96    |
| macro avg    | 0.81868   | 0.801087 | 0.808786 | 100     |
| weighted avg | 0.970889  | 0.96     | 0.964011 | 100     |

### Qualification level codebook

According to [this guidance on qualifications from the UK government](https://www.gov.uk/government/publications/early-years-qualifications-achieved-in-england), the types of qualifications are as follows:

|  Qualification Level |  Qualification Name                                                                                                                                                                                             |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 2                    | BIIAB Level 2 Diploma for the Early Years Practitioner                                                                                                                                                          |
| 2                    | City & Guilds Level 2 Diploma for the Early Years Practitioner (England)                                                                                                                                        |
| 2                    | Level 2 Diploma for the Early Years Practitioner                                                                                                                                                                |
| 2                    | Level 2 Diploma for the Early Years Practitioner                                                                                                                                                                |
| 2                    | NCFE CACHE Level 2 Diploma for the Early Years Practitioner                                                                                                                                                     |
| 2                    | BTEC Level 2 Diploma in Children's Play, Learning and Development (Early Years Practitioner)                                                                                                                    |
| 2                    | Pearson BTEC Level 2 Diploma for Early Years Practitioners                                                                                                                                                      |
| 2                    | Skillsfirst Level 2 Diploma for the Early Years Practitioner (RQF)                                                                                                                                              |
| 2                    | TQUK Level 2 Diploma for the Early Years Practitioner (RQF)                                                                                                                                                     |
| 2                    | Level 2 Children and Young People's Workforce (CYPW) Intermediate Apprenticeship Framework (early years pathway)                                                                                                |
| 3                    | BIIAB Level 3 Diploma in Children's Learning and Development (Early Years Educator)                                                                                                                             |
| 3                    | NCFE CACHE Level 3 Diploma in Early Years Education and Care (Early Years Educator)                                                                                                                             |
| 3                    | NCFE CACHE Level 3 Diploma for the Early Years Workforce ((Early Years Educator)                                                                                                                                |
| 3                    | NCFE CACHE Level 3 Diploma in Childcare and Education (Early Years Educator)                                                                                                                                    |
| 3                    | NCFE CACHE Level 3 Diploma in Montessori Pedagogy – Birth to Seven (Early Years Educator)                                                                                                                       |
| 3                    | NCFE CACHE Level 3 Diploma in Holistic Baby and Child Care (Early Years Educator)                                                                                                                               |
| 3                    | City and Guilds Level 3 Diploma For the Early Years Practitioner (Early Years Educator)                                                                                                                         |
| 3                    | City and Guilds Level 3 Advanced Technical Diploma for the Early Years Practitioner (Early Years Educator) (540)                                                                                                |
| 3                    | City and Guilds Level 3 Advanced Technical Extended Diploma for the Early Years Practitioner (Early Years Educator) (1080)                                                                                      |
| 3                    | Level 3 Diploma in Holistic Baby and Child Care (Early Years Educator)                                                                                                                                          |
| 3                    | FAQ Level 3 Diploma in Early Years Education and Childcare (Early Years Educator)                                                                                                                               |
| 3                    | Focus Awards Level 3 Diploma for the Children's Workforce (Early Years Educator) (RQF)                                                                                                                          |
| 3                    | ICQ Level 3 Diploma in Children's Learning and Development (Early Years Educator) (RQF)                                                                                                                         |
| 3                    | ICQ Level 3 Diploma for the Early Years Educator                                                                                                                                                                |
| 3                    | IAO Level 3 Diploma In Early Learning and Childcare (Early Years Educator)                                                                                                                                      |
| 3                    | NCFE Diploma for the Children's Workforce (Early Years Educator)                                                                                                                                                |
| 3                    | NCFE CACHE  Technical Level 3 Diploma in Early Years Education and Care (Early Years Educator)                                                                                                                  |
| 3                    | NCFE CACHE Technical Level 3 Diploma in Childcare and Education (Early Years Educator)                                                                                                                          |
| 3                    | T Level Technical Qualification in Education and Childcare (Specialism - Early Years Educator)                                                                                                                  |
| 3                    | Pearson BTEC Level 3 National Certificate in Children's Play, Learning and Development (Early Years Educator)                                                                                                   |
| 3                    | Pearson BTEC Level 3 National Diploma in Children's Play, Learning and Development (Early Years Educator)                                                                                                       |
| 3                    | Pearson Edexcel Level 3 Diploma in Children’s Learning and Development (Early Years Educator)                                                                                                                   |
| 3                    | Pearson BTEC Level 3 National Diploma in Children's Play, Learning and Development (Early Years Educator)                                                                                                       |
| 3                    | Pearson BTEC Level 3 National Extended Diploma in Children's Play, Learning and Development (Early Years Educator)                                                                                              |
| 3                    | Diploma for the Children and Young Peoples Workforce (Early Years Educator) (QCF)                                                                                                                               |
| 3                    | Diploma for the Children and Young People's Workforce (Early Years Educator) (QCF)                                                                                                                              |
| 3                    | Skillfirst Level 3 Diploma for the Children and Young People's Workforce (Early Years Educator) (RQF)                                                                                                           |
| 3                    | TQUK Level 3 Diploma for the Children's Workforce (Early Years Educator) (RQF)                                                                                                                                  |
| 4                    | Level 4 Diploma in Steiner Waldorf Early Childhood Studies (Early Years Educator)                                                                                                                               |
| 4                    | Certificate of Higher Education (Cert HE) in Montessori Early Childhood Practice                                                                                                                                |
| 4                    | NCFE CACHE Level 4 Diploma Steiner Waldorf Early Childhood Studies (Early Years Educator)                                                                                                                       |
| 4                    | NCFE CACHE Level 4 Diploma in Montessori Pedagogy – Birth to Seven (Early Years Educator)                                                                                                                       |
| 4                    | Pearson BTEC Level 4 HNC Diploma in Advanced Practice in Early Years Education                                                                                                                                  |
| 4                    | Pearson BTEC Level 4 Higher National Certificate in Early Childhood Education and Care                                                                                                                          |
| 5                    | Level 5 Diploma in Steiner Waldorf Early Childhood Studies – Leadership and Management                                                                                                                          |
| 5                    | City and Guilds Level 5 Diploma in Leadership for the Children's & Young People's Workforce - Early Years (Management)                                                                                          |
| 5                    | FdA Early Years and Education                                                                                                                                                                                   |
| 5                    | FD Early Childhood Studies                                                                                                                                                                                      |
| 5                    | Foundation Degree in Montessori Early Childhood Practice                                                                                                                                                        |
| 5                    | NCFE CACHE Level 5 Diploma for the Early Years Senior Practitioner                                                                                                                                              |
| 5                    | Pearson BTEC Level 5 HND Diploma in Advanced Practice in Early Years Education                                                                                                                                  |
| 5                    | BTEC Level 5 Higher National Diploma in Early Childhood Education and Care<br>                                                                                                                                  |
| 5                    | FdA Children and Young People (Early Years Educator)                                                                                                                                                            |
| 5                    | Foundation Degree in Early Years                                                                                                                                                                                |
| 5                    | Foundation Degree in Early Years                                                                                                                                                                                |
| 5                    | Foundation Degree in Early Years (with Early Years Educator)                                                                                                                                                    |
| 5                    | FdA Early Years Studies (with Early Years Educator)                                                                                                                                                             |
| 5                    | Foundation Degree Arts Children and Young People - Pathway Early Years with Practitioner Status                                                                                                                 |
| 5                    | Foundation Degree (Arts) FdA Early Years                                                                                                                                                                        |
| 5                    | Foundation Degree Early Years                                                                                                                                                                                   |
| 5                    | FdA Early Years Practice                                                                                                                                                                                        |
| 5                    | Learning and Development from Early Years to Adolescence (0-19) FdA                                                                                                                                             |
| 5                    | FdA Early Years (0-8yrs) (Professional Practice)                                                                                                                                                                |
| 5                    | Early Years Lead Practitioner (Level 5)                                                                                                                                                                         |
| 5                    | Foundation Degree in Early Years Educator                                                                                                                                                                       |
| 5                    | Foundation Degree in Early Childhood and Education (Integrated Working with Families and Children)                                                                                                              |
| 5                    | FdEd in Professional Practice in Early Years 0-8 (with Early Years Educator)                                                                                                                                    |
| 6                    | BA in Education Studies Early Years Educator Specialised Award                                                                                                                                                  |
| 6                    | BA (Hons) Childhood Studies                                                                                                                                                                                     |
| 6                    | BA (Hons) Joint Honours in Education: Education Studies and Early Years                                                                                                                                         |
| 6                    | BA (Hons) Joint Honours in Education: Early Years and Special and Inclusive Education                                                                                                                           |
| 6                    | BA (Hons) Joint Honours in Education: Early Years, Psychology and Education                                                                                                                                     |
| 6                    | BA (Hons) Joint Honours in Education: Early Years, Business and Education                                                                                                                                       |
| 6                    | BA (Hons) Childhood Studies                                                                                                                                                                                     |
| 6                    | FdA Early Years Professional                                                                                                                                                                                    |
| 6                    | BA (Hons) Education and Early Years                                                                                                                                                                             |
| 6                    | BA (Hons) Early Years and BA (Hons) Early Years (Top up)                                                                                                                                                        |
| 6                    | BA (Hons) Early Childhood Education                                                                                                                                                                             |
| 6                    | BA (Hons) Early Childhood Studies                                                                                                                                                                               |
| 6                    | Degrees and honours degrees at Level 6<br>which are fully consistent with the QAA subject benchmark statement for Early Childhood Studies and include an element of assessed practice in an early years setting |
| 6                    | Qualified Teacher Status (QTS)                                                                                                                                                                                  |
| 6                    | Early Years Teacher Status (EYTS)                                                                                                                                                                               |
| 6                    | Early Years Professional Status (EYPS)                                                                                                                                                                          |
