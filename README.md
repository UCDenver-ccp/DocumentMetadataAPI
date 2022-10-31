# DocumentMetadataAPI

Simple API for retrieving document metadata by PMID, PMC id, and/or DOI.

Meets the proposed specifications:
```
REQUEST SPECIFICATION: 
GET /publications?pubids=PMID:30690000,PMID:82374,PMID:28736,PMID:8000234&request_id=1df88223-c0f8-47f5-a1f3-661b944c7849
- Expect up to 100 pubids in request
- the value of request_id should be roundtripped back in the metadata section
- SLO for p90 should be 150ms for a good user experience
- Caching of results server-side is recommended to achieve this SLO
- Return 400 in case of request error; 500 in case of server error; 200 if success
- 'Content-Type: application/json' is expected as a header response; no other headers are mandated at this time
*/

// RESPONSE SPECIFICATION:
{
    // Metadata object
    "_meta": {
        // String, ISO 8601 in UTC, Time of response 
        "timestamp": "2022-11-27T14:45:07Z",
        // Non-negative integer, # of items in results
        "n_results": 2,
        // string, ID for the request, should be roundtripped from value in the request
        "request_id": "1df88223-c0f8-47f5-a1f3-661b944c7849",
   	 // Non-negative integer, time to process result in milliseconds
	 "processing_time_ms": 125
    },

    // Object, each key is a pubid submitted in the request
    // Can be empty object ({} instead of null) if no results found
    "results": {
        "PMID:30690000": {
            // Strings. When no value is present, use empty string instead of null
            // Send month names as capitalized three-letter abbreviations ("Dec")
            // Cf. https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmode=XML&version=2.0&id=30690000
            "journal_name": "European journal of pharmacology",
            "journal_abbrev": "Eur J Pharmacol",
            "article_title": "Stable gastric pentadecapeptide BPC 157 in the therapy of the rats with bile duct ligation.",
            "volume": "847",
            "issue": "",
            "pub_year": "2019",
            "pub_month": "Mar",
            "pub_day": "15",
            "abstract": "Recently, stable gastric pentadecapeptide BPC 157 reversed the high MDA- and NO-tissue values to the healthy levels. Thereby, BPC 157 therapy cured rats with bile duct ligation (BDL) (sacrifice at 2, 4, 6, 8 week). BPC 157-medication (10 μg/kg, 10 ng/kg) was continuously in drinking water (0.16 μg/ml, 0.16 ng/ml, 12 ml/rat/day) since awakening from surgery, or since week 4. Intraperitoneal administration was first at 30 min post-ligation, last at 24 h before sacrifice. Local bath BPC 157 (10 µg/kg) with assessed immediate normalization of portal hypertension was given immediately after establishing portal hypertension values at 4, 6, 8 week. BPC 157 therapy markedly abated jaundice, snout, ears, paws, and yellow abdominal tegmentum in controls since 4th week, ascites, nodular, steatotic liver with large dilatation of main bile duct, increased liver and/or cyst weight, decreased body weight. BPC 157 counteracts the piecemeal necrosis, focal lytic necrosis, apoptosis and focal inflammation, disturbed cell proliferation (Ki-67-staining), cytoskeletal structure in the hepatic stellate cell (α-SMA staining), collagen presentation (Mallory staining). Likewise, counteraction includes increased AST, ALT, GGT, ALP, total bilirubin, direct and indirect and decreased albumin serum levels. As the end-result appear normalized MDA- and NO-tissue values, next to Western blot of NOS2 and NOS3 in the liver tissue, and decreased IL-6, TNF-α, IL-1β levels in liver tissue. Finally, although portal hypertension is sustained in BDL-rats, with BPC 157 therapy, portal hypertension in BDL-rats is either not even developed or rapidly abated, depending on the given BPC 157's regimen. Thus, BPC 157 may counteract liver fibrosis and portal hypertension."
        },
        "PMID:8000234": {
            // Cf. https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&retmode=XML&version=2.0&id=8000234
            "journal_name": "Bulletin du Groupement international pour la recherche scientifique en stomatologie & odontologie",
            "journal_abbrev": "Bull Group Int Rech Sci Stomatol Odontol",
            "article_title": "Microanatomic features of unilateral condylar hyperplasia.",
            "volume": "37",
            "issue": "3-4",
            "pub_year": "1994",
            "pub_month": "Sep-Dec",
            "pub_day": "",
            "abstract": "Microanatomic features of unilateral condylar hyperplasia (UCH) are described. The articular surface exhibited clefts with surrounding elevations, and globules varying 0.5-2 microns in diameter. The articular zone presented giant coiled fibers, and the proliferative zone was composed of small round cells. The findings suggest that degenerative changes occur in UCH, both in adult and juvenile forms."
        }
    }
    // Array of strings, each element is a pubid from the request that could not be located;
    // Can be empty array ([] instead of null) if all IDs were found
    "not_found": [
        "PMID:82374", "PMID:28736"
    ]
}

```