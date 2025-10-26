# -------------------------
# Prompt template and LLM call
# -------------------------
PROMPT_INSTRUCTION = """
INSTRUCTIONS (READ CAREFULLY) — STRICT JSON-ONLY OUTPUT

You will convert ONE scraped product JSON (given below) into exactly ONE JSON object whose keys match the CSV header below **exactly** (including spaces and punctuation).  
Return **only** the JSON object — no explanation, no commentary, no code fences.

TEMPLATE CSV HEADER (keys MUST match these strings exactly, in this order):
["platform","date","product ID","name","scientific name","product form","net quantity","unit","price","price per unit","seller name","seller type","seller origin","number of reviews","average rating","sales rank or badge","certifications","claims","transparency origin","cold pressed","steam distilled","refined","list of ingredients","image","product description","return policy","collection method"]

OUTPUT FORMAT REQUIREMENTS
- Return a single JSON object with the keys shown above, no extra keys.
- Keys must be exactly the same string as the header (including spaces).
- Types:
  - Strings: use JSON strings or `null`.
  - Numbers: use JSON numbers (no commas). If unknown -> `null`.
  - Booleans: `true` or `false` (logical).
  - Arrays: JSON arrays (e.g., for "list of ingredients").
- Date format for "date": ISO date `YYYY-MM-DD`. If input has a date (uploadDate, listedOn, lastUpdated), convert it to ISO date. If none, put `null`.
- If you cannot determine a value, set it to `null` (do not invent).
- For fields that must be inferred, follow the rules below exactly.

FIELD-BY-FIELD RULES / INFERENCE LOGIC
- "platform": short name of the source site (e.g., "amazon", "ebay", "etsy", "alibaba", "jumia", "walmart"). If URL present, infer from domain.
- "date": prefer these fields (in order) if present in input: `uploadDate`, `listedOn`, `lastUpdated` (parse and convert). If the value contains only a timestamp, convert to date. Else `null`.
- "product ID": use the record's SKU/ID/itemNumber/productId/sku/id field (first available). String.
- "name": canonical product title/name.
- "scientific name": botanical/scientific name if explicitly present in title/description or brand notes (e.g., "Moringa oleifera"); otherwise `null`.
- "product form": choose exactly one of these strings (lowercase): `oil`, `powder`, `extract`, `capsule`, `tablet`, `seed`, `butter`, `liquid`, `other`. Determine from `type`, `title`, `description`, `variations`, or keywords like "oil","powder","extract","capsule","tablet","seed","butter","liquid". If ambiguous, pick best match; if not match, `other`.
- "net quantity": numeric quantity if present (e.g., "2 oz" -> 2, "60 count" -> 60). If not inferable, `null`.
- "unit": unit string for net quantity (e.g., "g", "kg", "oz", "ml", "count", "pcs", "l"). If not determined, `null`.
- "price": numeric price in local currency (use the primary price field; remove currency symbols and thousand separators). If multiple price ranges present, use the single-item `price` or smallest numeric price. If unknown, `null`.
- "price per unit": numeric price / net quantity when both are known and unit is consistent; otherwise `null`.
- "seller name": seller/publisher/manufacturer name (from `seller`, `creator`, `brand`, or `supplierWebsite`).
- "seller type": infer one of the strings: `"manufacturer"`, `"wholesaler"`, `"retailer"`, `"marketplace"`, `"other"`. Use supplierWebsite domain or fields like "supplier", "company", or presence of "manufacturer" or "supplier" in description. If uncertain, return `"other"`.
- "seller origin": country name (e.g., "United States") inferred from seller location fields (itemLocation, supplierUrl domain TLD, or address in text). If cannot determine, `null`.
- "number of reviews": numeric total reviews (e.g., `numberOfReviews`, `review_count`, `totalRatings`). If not present, `null`.
- "average rating": numeric rating value (e.g., `reviewRatingValue`, `rating.average`). If not present, `null`.
- "sales rank or badge": textual badge like "Best Seller", "Top Rated", "% sold" or "318 sold" summary; prefer explicit badge fields, else derive short text from sold counts (e.g., "318 sold") or isSponsored flag. If none, `null`.
- "certifications": array of strings (e.g., ["organic","non-GMO","GMP"]) if found in title/description/labels; else `[]`.
- "claims": array of short claims found in title/description/labels (e.g., ["non-GMO","vegan"]) or `[]`.
- "transparency origin": any explicit origin statement like "Made in USA" or supply-chain notes (string) if found in description or supplier fields; otherwise `null`.
- "cold pressed": boolean `true` if description/title contains "cold pressed" or "cold-pressed"; else `false`.
- "steam distilled": boolean `true` if description/title contains "steam distilled" or "steam-distilled"; else `false`.
- "refined": boolean `true` if description contains "refined" or "refinement"; else `false`.
- "list of ingredients": array of ingredient names parsed from description or title; if none, empty array `[]`.
- "image": main image URL string (pick `image` field or first entry in `images`/`additionalPhotosArray`).
- "product description": cleaned textual description (trimmed to plain text; remove extraneous HTML if present). If no description, `null`.
- "return policy": string from shipping/returns fields. If none, `null`.
- "collection method": infer one of `["cold pressed","steam distilled","expeller pressed","solvent extracted","other","unknown"]` based on description keywords. If description contains "cold", choose "cold pressed"; if "steam", choose "steam distilled"; if "expeller", "expeller pressed"; if "solvent" / "hexane" -> "solvent extracted"; otherwise "unknown".

ADDITIONAL INSTRUCTIONS
- Arrays must be JSON arrays. If you have no entries for "certifications" or "claims" or "list of ingredients", return an empty JSON array `[]`.
- Use lowercase for "product form" and "collection method" values exactly as listed above.
- Use the scientific name of the base plant-based ingredient
- Keep numeric fields as numbers (no quotes).
- For textual arrays (certifications, claims, list of ingredients), remove duplicates and trim whitespace.
- Do not return nested objects (all values must be primitive, number, boolean, string, or array).
- If the input contains URLs, domains may be used to infer seller origin (e.g., `.ng` -> Nigeria). Country inference should be the full English country name.
- Exactly one JSON object must be returned and nothing else.

CONTEXT: the single product JSON to normalize is provided below. Process it according to the rules above.

<<<INPUT_PRODUCT_JSON>>>

EXAMPLES (these are required output formats — replicate this style exactly):

1) Alibaba sample input -> example output
Input (summarized context): the Alibaba product has fields: name, sku, brand, link, description, image, price, uploadDate, creator, additionalPhotosArray, priceRange, minOrder.
Expected normalized JSON output:
{"platform":"Alibaba","date":"2025-10-24","product ID":"1600906122423","name":"Bulk Hot Sale Rosehip Castor Jojoba Avocado Moringa Seed Argan Emu Aloe Vera Camellia Seeds Oil New Carrier Oil","scientific name":"Ricinus communis","product form":"oil","net quantity":2,"unit":"pcs","price":489.18,"price per unit":244.59,"seller name":"Ji'an Borui Spice Oil Co., Ltd.","seller type":"manufacturer","seller origin":null,"number of reviews":null,"average rating":null,"sales rank or badge":null,"certifications":["organic"],"claims":["bulk","dropshipping available","free shipping"],"transparency origin":null,"cold pressed":false,"steam distilled":false,"refined":null,"list of ingredients":["rosehip oil","castor oil","jojoba oil","avocado oil","moringa seed oil","argan oil","emu oil","aloe vera oil","camellia seed oil"],"image":"https://sc04.alicdn.com/kf/H45c6e2bf50e3497e9a5e22b3d6481bc6H.jpg","product description":"Bulk Hot Sale Rosehip Castor Jojoba Avocado Moringa Seed Argan Emu Aloe Vera Camellia Seeds Oil New Carrier Oil , Find Complete Details about Bulk Hot Sale ...","return policy":null,"collection method":"unknown"}

2) eBay sample input -> example output
{"platform":"Ebay","date":null,"product ID":"266609431231","name":"Global Healing Organic Moringa Liquid Supplement - Non-GMO, Vegan Friendly - 2oz","scientific name":"Moringa oleifera","product form":"extract","net quantity":2,"unit":"oz","price":24.95,"price per unit":12.475,"seller name":"Global Healing Center","seller type":"retailer","seller origin":"United States","number of reviews":null,"average rating":null,"sales rank or badge":"318 sold","certifications":["non-GMO"],"claims":[],"transparency origin":null,"cold pressed":false,"steam distilled":false,"refined":null,"list of ingredients":[],"image":"https://i.ebayimg.com/images/g/Q1IAAOSwjVNlg8Fa/s-l1000.webp","product description":null,"return policy":null,"collection method":"other"}

3) Etsy sample input -> example output
{"platform":"Etsy","date":"2025-10-17","product ID":"4388599500","name":"Oil of Oregano, Black Seed & Moringa Softgels 3 in 1 – 60 Vegan Capsules | Natural Wellness Blend | Non-GMO","scientific name":"Moringa oleifera","product form":"capsule","net quantity":60,"unit":"count","price":58.99,"price per unit":0.9831666666666667,"seller name":"HerbalOrganicWork","seller type":"retailer","seller origin":null,"number of reviews":null,"average rating":null,"sales rank or badge":null,"certifications":["non-GMO"],"claims":["vegan"],"transparency origin":null,"cold pressed":false,"steam distilled":false,"refined":null,"list of ingredients":[],"image":"https://i.etsystatic.com/62129839/r/il/a80af3/7296497478/il_794xN.7296497478_dmzo.jpg","product description":null,"return policy":null,"collection method":"other"}

4) Jumia sample input -> example output
{"platform":"Jumia","date":"2025-10-25","product ID":"FA203MW694IUFNAFAMZ","name":"Quality Italian 7star Cashmere SuperWool Senator Fabric Material: Light Onion Color(4yards)","scientific name":null,"product form":"other","net quantity":4,"unit":"yards","price":28000,"price per unit":7000,"seller name":null,"seller type":"retailer","seller origin":null,"number of reviews":0,"average rating":0,"sales rank or badge":null,"certifications":[],"claims":[],"transparency origin":null,"cold pressed":false,"steam distilled":false,"refined":null,"list of ingredients":[],"image":"https://ng.jumia.is/unsafe/fit-in/300x300/filters:fill(white)/product/78/1246473/1.jpg?7905","product description":null,"return policy":null,"collection method":"other"}

5) Walmart sample input -> example output
{"platform":"Walmart","date":null,"product ID":"49586260","name":"Organic Shea Butter by Now Foods - 7 Ounces","scientific name":"Butyrospermum parkii","product form":"butter","net quantity":7,"unit":"oz","price":12.99,"price per unit":1.8557142857142858,"seller name":"The Fruitful Yield, Inc.","seller type":"retailer","seller origin":"United States","number of reviews":21,"average rating":4.3,"sales rank or badge":null,"certifications":["USDA Organic"],"claims":["100% pure","moisturizing","emollient","vitamin enriched"],"transparency origin":"Derived from karite trees in Western and Central Africa","cold pressed":false,"steam distilled":false,"refined":null,"list of ingredients":["Organic Butyrospermum Parkii (Shea) Butter"],"image":"[https://i5.walmartimages.com/seo/NOW-Foods-Organic-Shea-Butter-7-fl-oz-Solid-Oil_1fa1cc86-cc96-42eb-9558-a33582704a94.02be8fd38738688752e1582f5e2e1657.jpeg","product](https://i5.walmartimages.com/seo/NOW-Foods-Organic-Shea-Butter-7-fl-oz-Solid-Oil_1fa1cc86-cc96-42eb-9558-a33582704a94.02be8fd38738688752e1582f5e2e1657.jpeg%22,%22product) description":"Condition: Dry, cracked or chapped skin in need of moisture, especially on tougher areas such as the elbows, knees and feet. Solution: 100% Pure & Certified Organic Shea Butter has a rich, luxurious texture that penetrates deep to condition and moisturize every type of skin. Shea Butter is derived from the tree nuts of the karite trees that grow in Western and Central Africa. It is a wonderful emollient that's perfect for daily use. Can also be used as a scalp moisturizer.","return policy":"Free 90-day returns","collection method":"other"}

6) Amazon sample input -> example output
{"platform":"Amazon","date":null,"product ID":"B0CF6SKMH1","name":"Kyabo 100% Pure and All Natural Cocoa Butter - 3lb - Food Grade - great for making lip balm, cream, hair products, candle, hair removal and craft projects - Made with Organic Cacao","scientific name":"Theobroma cacao","product form":"butter","net quantity":3,"unit":"lb","price":77.95,"price per unit":25.983333333333334,"seller name":"kyabo Organics","seller type":"manufacturer","seller origin":"United States","number of reviews":105,"average rating":4.6,"sales rank or badge":"#78,241 in Beauty & Personal Care; #246 in Body Butters","certifications":["organic"],"claims":["100% pure","vegan","keto-friendly","ethically sourced","food grade"],"transparency origin":null,"cold pressed":false,"steam distilled":false,"refined":null,"list of ingredients":["Cocoa Butter"],"image":"[https://m.media-amazon.com/images/I/41VMno9CISL._SY300_SX300_QL70_FMwebp_.jpg","product](https://m.media-amazon.com/images/I/41VMno9CISL._SY300_SX300_QL70_FMwebp_.jpg%22,%22product) description":"Cocoa Butter is a easily absorbed Body Butter leaving your skin smooth and soft for up to 48hrs after application! Body Moisturizer that will soften skin. For dry / very dry skin 48hr Hydration - Easily absorbed Rich in Vitamin E, Prevents & Treats Stretch Marks. Soft, smooth and easy to directly to skin apply. Great addition to your recipe for making lotion, cream, lip balm etc. Shelf Life: This butter should be stored in a cool, dark place and has a shelf-life of 2 years when stored properly.","return policy":null,"collection method":"other"}

Return the normalized JSON now.

END OF PROMPT
"""
