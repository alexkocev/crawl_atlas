  // TO BE USED IN GGSHEET TO MERGE main_clinics with gmaps data

  // --- Toggle / menu setup ---
  // Cell that triggers the merge when checked (sheet name + cell, e.g. "main_clinics!A1")
  const TRIGGER_SHEET = "main_clinics";
  const TRIGGER_CELL = "A1";

  function onOpen() {
    SpreadsheetApp.getUi()
      .createMenu("🎯 Merge Clinics")
      .addItem("1. Populate gmaps_clinics_clean", "populateGmapsClinicsClean")
      .addItem("2. Run merge (refresh all_clinics)", "refreshAllClinics")
      .addToUi();
  }

  function onEdit(e) {
    if (!e || !e.range) return;
    const sheet = e.range.getSheet();
    if (sheet.getSheetName() !== TRIGGER_SHEET) return;
    if (e.range.getA1Notation() !== TRIGGER_CELL) return;
    if (e.range.getValue() !== true) return;  // Checkbox must be checked
    e.range.setValue(false);  // Reset checkbox immediately (avoids re-trigger)
    refreshAllClinics();
  }

// ✅ KEEP rows where type_label matches at least one of these label types (case-insensitive)

  const LABEL_TYPES = [
    "chiropractor",
    "physical therapist",
    "medical clinic",
    "dental clinic",
    "medical center",
    "doctor",
    "skin care clinic",
    "massage",
    "wellness center",
    "massage spa",
    "pharmacy",
    "medical laboratory"
    // Excluded: Health Food Store (retail, not clinical)
  ].map(k => k.toLowerCase());


  // Australian state/territory abbreviation → full name


  const STATE_ABBREV_TO_FULL = {
    "NSW": "New South Wales",
    "VIC": "Victoria",
    "QLD": "Queensland",
    "WA": "Western Australia",
    "SA": "South Australia",
    "TAS": "Tasmania",
    "NT": "Northern Territory",
    "ACT": "Australian Capital Territory"
  };

  function expandState(abbrev) {
    if (!abbrev || typeof abbrev !== "string") return abbrev || "";
    const trimmed = String(abbrev).trim();
    const upper = trimmed.toUpperCase();
    return STATE_ABBREV_TO_FULL[upper] || trimmed;
  }

  function cleanEmails(value) {
    if (!value || typeof value !== "string") return "";
    let s = String(value).trim();
    if (!s) return "";
    return s.split(/[,;]+/)
      .map(addr => addr.trim().replace(/%20/g, ""))
      .filter(addr => addr && !addr.includes("*"))
      .join(", ");
  }

  // Force long IDs (cid, place_id) as text so Google Sheets doesn't show scientific notation
  function formatAsTextId(value) {
    if (value === null || value === undefined || value === "") return "";
    const str = String(value).trim();
    if (!str) return "";
    return "'" + str;  // Leading apostrophe forces Sheets to store as text
  }

  function normalizeUrl(url) {
    if (!url) return "";
    let s = url.toString().toLowerCase()
      .replace(/^https?:\/\//, '')
      .replace(/\/$/, '')
      .trim();
    if (!s) return "";
    // Remove UTM parameters (e.g. ?utm_source=google&utm_medium=gbp&utm_campaign=...)
    const qIdx = s.indexOf('?');
    if (qIdx !== -1) {
      const base = s.slice(0, qIdx);
      const query = s.slice(qIdx + 1).split('#')[0];
      const nonUtmParams = query.split('&').filter(p => {
        const key = (p.split('=')[0] || '').trim();
        return key && !key.startsWith('utm_');
      });
      s = base + (nonUtmParams.length ? '?' + nonUtmParams.join('&') : '');
      s = s.replace(/\/$/, '');  // Remove trailing slash if query was stripped
    }
    if (!s) return "";
    if (!s.startsWith('www.')) s = 'www.' + s;
    return s;
  }

  // Columns to extract from gmaps_clinics (output order: website first, then these)
  const targetOutFields = [
    "name", "place_id", "cid", "type_label", "phone_international",
    "street", "city", "state", "postal_code", "country",
    "latitude", "longitude", "rating", "reviews",
    "wheelchair_entrance", "opening_hours"
  ];
  // Fallbacks for gmaps column name variations
  const GMAPS_FIELD_ALIASES = {
    "type_label": ["type_label", "type"],
    "phone_international": ["phone_international", "phone"],
    "opening_hours": ["opening_hours", "working_hours_csv_compatible"]
  };
  function getGmapsColIndex(headers, field) {
    const aliases = GMAPS_FIELD_ALIASES[field] || [field];
    for (const alias of aliases) {
      const idx = headers.indexOf(alias);
      if (idx !== -1) return idx;
    }
    return headers.indexOf(field);
  }

  /**
   * 1. Populate gmaps_clinics_clean sheet
   * Reads gmaps_clinics, extracts needed fields, filters on types/business_status, cleans URLs etc.
   * @param {boolean} silent - If true, skip success alert (e.g. when called from refreshAllClinics)
   */
  function populateGmapsClinicsClean(silent) {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const gmapsSheet = ss.getSheetByName("gmaps_clinics");
    if (!gmapsSheet) {
      SpreadsheetApp.getUi().alert("Error: Sheet 'gmaps_clinics' not found.");
      return;
    }

    const outData = gmapsSheet.getDataRange().getValues();
    if (outData.length < 1) {
      SpreadsheetApp.getUi().alert("Error: gmaps_clinics is empty.");
      return;
    }

    const outHeaders = outData[0];
    const outWebsiteIndex = outHeaders.indexOf("website");
    const outBusinessStatusIndex = outHeaders.indexOf("business_status");

    if (outWebsiteIndex === -1) {
      SpreadsheetApp.getUi().alert("Error: Could not find 'website' column in gmaps_clinics.");
      return;
    }

    const outFieldIndices = targetOutFields.map(field => getGmapsColIndex(outHeaders, field));

    const cleanHeaders = ["website"].concat(targetOutFields);
    const cleanRows = [];

    for (let i = 1; i < outData.length; i++) {
      const row = outData[i];
      const rawUrl = row[outWebsiteIndex];
      const cleanUrl = normalizeUrl(rawUrl);
      if (!cleanUrl) continue;

      const businessStatus = outBusinessStatusIndex !== -1
        ? String(row[outBusinessStatusIndex] || "").trim()
        : "";
      if (businessStatus !== "OPERATIONAL") continue;

      const typeLabelIdx = targetOutFields.indexOf("type_label");
      const typeColIndex = typeLabelIdx !== -1 ? outFieldIndices[typeLabelIdx] : -1;
      const typeVal = (typeColIndex !== -1 && row[typeColIndex])
        ? String(row[typeColIndex]).trim().toLowerCase()
        : "";
      if (!LABEL_TYPES.some(lt => typeVal.includes(lt))) continue;

      const extractedData = targetOutFields.map((field, idx) => {
        const colIndex = outFieldIndices[idx];
        let val = colIndex !== -1 ? row[colIndex] : "";
        if ((field === "cid" || field === "place_id") && val) val = formatAsTextId(val);
        return val;
      });

      cleanRows.push([cleanUrl].concat(extractedData));
    }

    let cleanSheet = ss.getSheetByName("gmaps_clinics_clean");
    if (!cleanSheet) {
      cleanSheet = ss.insertSheet("gmaps_clinics_clean");
    }
    cleanSheet.clear();
    const cleanData = [cleanHeaders, ...cleanRows];
    const numRows = cleanData.length;
    const numCols = cleanData[0].length;
    cleanSheet.getRange(1, 1, numRows, numCols).setValues(cleanData);
    cleanSheet.setFrozenRows(1);
    if (!silent) SpreadsheetApp.getUi().alert("Success! gmaps_clinics_clean has been populated.");
  }

function refreshAllClinics() {
  populateGmapsClinicsClean(true);

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const mainSheet = ss.getSheetByName("main_clinics");
  const cleanSheet = ss.getSheetByName("gmaps_clinics_clean");
  const allSheet = ss.getSheetByName("all_clinics");

  if (!mainSheet || !cleanSheet || !allSheet) {
    SpreadsheetApp.getUi().alert("Error: Make sure you have sheets named 'main_clinics', 'gmaps_clinics_clean', and 'all_clinics'.");
    return;
  }

  const mainData = mainSheet.getDataRange().getValues();
  const cleanData = cleanSheet.getDataRange().getValues();

  if (mainData.length < 1 || cleanData.length < 1) {
    SpreadsheetApp.getUi().alert("Error: One of your source sheets is empty.");
    return;
  }

  const mainHeaders = mainData[0];

  const mainUrlIndex = mainHeaders.indexOf("website_url");
  if (mainUrlIndex === -1) {
    SpreadsheetApp.getUi().alert("Error: Could not find 'website_url' column in main_clinics.");
    return;
  }

  // Filter main_clinics headers (remove 'error_log' and anything containing 'Column')
  const mainIndicesToKeep = [];
  const filteredMainHeaders = [];
  
  for (let i = 0; i < mainHeaders.length; i++) {
    const headerName = String(mainHeaders[i]);
    if (headerName === "error_log" || headerName.toLowerCase().includes("column")) continue;
    mainIndicesToKeep.push(i);
    filteredMainHeaders.push(headerName);
  }

  const scrapingDateIndex = filteredMainHeaders.indexOf("scraping_date");
  const emailsColIndexInFiltered = filteredMainHeaders.indexOf("emails");

  // Build gmaps lookup map from gmaps_clinics_clean (already filtered & cleaned)
  // Clean sheet columns: website, name, place_id, cid, type_label, phone_international, street, city, state, postal_code, country, latitude, longitude, rating, reviews, wheelchair_entrance, opening_hours
  const gmapsMap = {};
  const nTargetFields = targetOutFields.length;
  for (let i = 1; i < cleanData.length; i++) {
    const row = cleanData[i];
    const cleanUrl = String(row[0] || "").trim();
    if (!cleanUrl) continue;

    const extractedData = row.slice(1, 1 + nTargetFields);

    gmapsMap[cleanUrl] = {
      fields: extractedData
    };
  }

  // Merge the data (unsorted first, then we'll reorder)
  const allHeaders = filteredMainHeaders.concat(targetOutFields);
  const allRows = [];

  for (let i = 1; i < mainData.length; i++) {
    const row = mainData[i];

    // Skip rows with no scraping_date
    if (scrapingDateIndex !== -1) {
      const scrapingDateColInMain = mainIndicesToKeep[scrapingDateIndex];
      if (!row[scrapingDateColInMain] || String(row[scrapingDateColInMain]).trim() === "") continue;
    }

    const rawUrl = row[mainUrlIndex];
    const cleanUrl = normalizeUrl(rawUrl);
    const filteredMainRow = mainIndicesToKeep.map(idx => row[idx]);
    // Normalize website_url in output (www. only, no http/https)
    const websiteUrlColInFiltered = filteredMainHeaders.indexOf("website_url");
    if (websiteUrlColInFiltered !== -1 && cleanUrl) {
      filteredMainRow[websiteUrlColInFiltered] = cleanUrl;
    }

    let outFields = Array(targetOutFields.length).fill("");

    const match = gmapsMap[cleanUrl];
    if (!match) continue;  // Only include rows that have a gmaps match (all are OPERATIONAL)

    outFields = match.fields;
    // Expand state abbreviation (e.g. NSW → New South Wales)
    const stateIdx = targetOutFields.indexOf("state");
    if (stateIdx !== -1 && outFields[stateIdx]) {
      outFields[stateIdx] = expandState(outFields[stateIdx]);
    }

    // Clean emails: remove %20, drop addresses containing *
    if (emailsColIndexInFiltered !== -1) {
      filteredMainRow[emailsColIndexInFiltered] = cleanEmails(filteredMainRow[emailsColIndexInFiltered]);
    }

    allRows.push(filteredMainRow.concat(outFields));
  }

  // --- Column reordering ---
  const desiredOrder = [
    "name", "website_url", "type", "billing_type", "practitioner_count",
    "business_status", "rating", "reviews", "home_visits",
    "pms_stack", "booking_type",
    "booking_stack", "telehealth_stack",
    "payments_stack", "crm_stack", "forms_stack", "live_chat_stack", "email_provider_stack", "cms_stack", "infra_stack",
    "pixels_stack", "reviews_stack", "phone", "emails", "working_hours_csv_compatible", "instagram",
    "whatsapp", "street", "city", "state", "postal_code", "country",
    "latitude", "longitude", "place_id", "cid", "scraping_date"
  ];

  // Map each desired column to its index in allHeaders; track which are found
  const orderedIndices = [];
  const usedIndices = new Set();

  for (const col of desiredOrder) {
    const idx = allHeaders.indexOf(col);
    if (idx !== -1) {
      orderedIndices.push(idx);
      usedIndices.add(idx);
    }
    // If not found, we skip it here — it simply won't appear (no data to add)
  }

  // Append any leftover columns not in the desired order and not already included
  for (let i = 0; i < allHeaders.length; i++) {
    if (!usedIndices.has(i)) orderedIndices.push(i);
  }

  // Build final headers and rows using the ordered indices
  const finalHeaders = orderedIndices.map(i => allHeaders[i]);
  const finalRows = allRows.map(row => orderedIndices.map(i => row[i] !== undefined ? row[i] : ""));

  // Write to all_clinics
  allSheet.clear();
  const allClinicsData = [finalHeaders, ...finalRows];
  const numRows = allClinicsData.length;
  const numCols = allClinicsData[0].length;
  allSheet.getRange(1, 1, numRows, numCols).setValues(allClinicsData);

  // --- Header colouring ---
  // We need to know which final columns came from which source
  const mainHeaderSet = new Set(filteredMainHeaders);
  const outHeaderSet = new Set(targetOutFields);

  for (let col = 0; col < finalHeaders.length; col++) {
    const header = finalHeaders[col];
    let color = null;
    if (mainHeaderSet.has(header)) color = "#C9DAF8";  // blue   — main_clinics
    else if (outHeaderSet.has(header)) color = "#FFF2CC";   // yellow — gmaps

    if (color) {
      allSheet.getRange(1, col + 1, 1, col + 1).setBackground(color).setFontWeight("bold");
    }
  }

  allSheet.setFrozenRows(1);
  SpreadsheetApp.getUi().alert("Success! The all_clinics sheet has been refreshed.");
}