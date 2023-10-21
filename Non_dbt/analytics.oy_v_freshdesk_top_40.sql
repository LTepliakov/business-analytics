drop view if exists analytics.oy_v_freshdesk_top_40
;
create view analytics.oy_v_freshdesk_top_40 as
select 			l.name as company , l.industry , r.*
from			(
				select * from raw.freshdesk__companies 
				where name in 	-- This list of names comes from https://unifonic.freshdesk.com/a/analytics/reports/NzU4ODI3/page/1
								(
								'ELM Company'
								, 'Saudi Airlines'
								, 'Saudi Electricity Company'
								, 'Alinma Bank'
								, 'Bank Al-Bilad'
								, 'Arab National Bank - ANB'
								, 'Bank Al Jazira'
								, 'Aramex Limited Company'
								, 'Al-Nahdi Medical Company'
								, 'National Unified Procurement Company for Medical Supplies - Nupco'
								, 'Hungerstation Ltd Co.'
								, 'United Electronics Company - Extra'
								, 'Najm Company for insurance services'
								, 'Daily Food Company'
								, 'AL-Dawaa Medical Services Co. Ltd. - DMSCO'
								, 'Mouwasat Medical Services Co.'
								, 'Saudi Manpower Solutions Co.'
								, 'Sondoq Alaibtikar for information and technology'
								, 'Emkan Finance Company'
								, 'National Housing Company'
								, 'Education and Training Evaluation Commission'
								, 'Takamol Holding'
								, 'Al-Qdrah for Communications and Information Technology Ltd Co. - Qdrah - Zid'
								, 'Hamad Al Manea Trading Co. Ltd.'
								, 'Panda Retail Company'
								, 'The Saudi Company for Electronic Information Exchange [Tabadul]'
								, 'Communications and Information Technology Commission - CITC'
								, 'Dr. Soliman Abdul Qader Fakih Hospital Company'
								, 'Sea of Products Trading Corporation - Nice One'
								, 'International Medical Center -IMC'
								, 'National Guard Health Affairs'
								, 'Takamol- Qiwa Project'
								, 'Aram Meem Trading Services Company - Toyou'
								, 'Tamkeentech - Tamkeen Technologies'
								, 'Saudi Abyat for Building Materials'
								, 'United Company for Financial Services'
								, 'Nakhla Information Systems Technology - Tamara'
								, 'Tabby Saudi for Communication and Information Technology Co.'
								, 'Abdul Latif Jameel United Finance Company'
								, 'Integrated Telecom Company Ltd. - Salam'
								)
				) as l 
left join		raw.freshdesk__tickets as r 
on 				l.id = r.company_id 
;