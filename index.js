const fs = require("fs");
const path = require("path");
const mysql = require("mysql2/promise");
const redis = require("redis");;
const XLSX = require("xlsx");
const csvParser = require('csv-parser');
const readline = require("readline");

// =========================
// READ FILES FROM data
// =========================
async function readClinicalNotesFromCSV(csvPath) {
  console.log("Reading clinical notes from CSV...");

  return new Promise((resolve, reject) => {
    const notes = new Map();

    fs.createReadStream(csvPath)
      .pipe(csvParser())
      .on('data', (row) => {
        const patientId = parseInt(row['Patient ID']);
        const clinicalNotes = row["Clinician's Notes"] || "";

        if (patientId && clinicalNotes) {
          notes.set(patientId, clinicalNotes);
        }
      })
      .on('end', () => {
        console.log(`✓ Loaded clinical notes for ${notes.size} patients\n`);
        resolve(notes);
      })
      .on('error', reject);
  });
}
function readMetadataFromJSON(metadataDir) {
  console.log("Reading metadata from JSON files...");

  const metadataPath = path.join(metadataDir, "metadata");

  if (!fs.existsSync(metadataPath)) {
    console.error(`Metadata directory not found: ${metadataPath}`);
    return [];
  }

  const files = fs.readdirSync(metadataPath).filter(f => f.endsWith('.json'));
  console.log(`Found ${files.length} JSON files (patients)\n`);

  const patientMetadata = [];

  for (const file of files) {
    try {
      const filePath = path.join(metadataPath, file);
      const content = fs.readFileSync(filePath, 'utf8');
      const data = JSON.parse(content);

      // Each file is an array of images for one patient
      if (Array.isArray(data) && data.length > 0) {
        patientMetadata.push({
          filename: file,
          images: data
        });
      }
    } catch (err) {
      console.error(`Error reading ${file}: ${err.message}`);
    }
  }

  console.log(`✓ Loaded ${patientMetadata.length} patient files`);
  const totalImages = patientMetadata.reduce((sum, p) => sum + p.images.length, 0);
  console.log(`✓ Total images: ${totalImages}\n`);

  return patientMetadata;
}

// =========================
// PROCESS METADATA INTO PATIENT STRUCTURE
// =========================
function processMetadataIntoPatients(patientMetadataFiles) {
  console.log("Processing patient metadata files...");

  const patients = [];

  for (const patientFile of patientMetadataFiles) {
    const firstImage = patientFile.images[0];

    // Extract patient ID from file path
    let patientId = null;
    const pathMatch = firstImage.File.match(/\\(\d{4})\\/);
    if (pathMatch) {
      patientId = parseInt(pathMatch[1], 10);
    } else {
      // Fallback: use filename
      const filenameMatch = patientFile.filename.match(/(\d+)/);
      patientId = filenameMatch ? parseInt(filenameMatch[1], 10) : patients.length + 1;
    }

    // Create patient record
    const patient = {
      patient_id: patientId,
      patient_name: firstImage.PatientName || `Patient_${String(patientId).padStart(4, '0')}`,
      patient_sex: firstImage.PatientSex || "",
      patient_age: firstImage.PatientAge || "",
      patient_size: firstImage.PatientSize || "",
      patient_weight: firstImage.PatientWeight || "",
      patient_birth_date: firstImage.PatientBirthDate || "",
      body_part_examined: firstImage.BodyPartExamined || "",
      clinical_notes: firstImage.StudyDescription || "",
      study_instance_uid: firstImage.StudyInstanceUID || "",
      images: [],
      total_images: 0,
      total_size: 0,
      modalities: new Set()
    };

    // Process all images
    for (const imgMeta of patientFile.images) {
      patient.images.push({
        file_path: imgMeta.File || "",
        file_name: path.basename(imgMeta.File || ""),
        file_size: 0, // Not available in JSON
        modality: imgMeta.Modality || "UNKNOWN",
        instance_number: imgMeta.InstanceNumber || "",
        series_number: imgMeta.SeriesNumber || "",
        acquisition_number: imgMeta.AcquisitionNumber || "",
        created_at: new Date(),
        metadata: imgMeta
      });

      patient.total_images++;
      patient.modalities.add(imgMeta.Modality || "UNKNOWN");
    }

    patient.modalities = Array.from(patient.modalities);
    patients.push(patient);
  }

  console.log(`✓ Processed ${patients.length} patients`);
  const totalImages = patients.reduce((sum, p) => sum + p.total_images, 0);
  console.log(`✓ Total images: ${totalImages}\n`);

  return patients;
}

// =========================
// MODIFIED INSERT DATA FUNCTION
// =========================
async function insertData(db, redisClient, patientData) {
  console.log("=".repeat(60));
  console.log("INSERTING DATA INTO DATABASES");
  console.log("=".repeat(60));

  const clinicalNotesMap = await readClinicalNotesFromCSV("./data/text_data.csv");

  let totalPatients = 0;
  let totalImages = 0;
  let totalMetadata = 0;
  const BATCH_SIZE = 50;

  for (let i = 0; i < patientData.length; i += BATCH_SIZE) {
    const batch = patientData.slice(i, i + BATCH_SIZE);

    for (let patient of batch) {
      const {
        patient_id,
        patient_name,
        patient_sex,
        patient_age,
        patient_size,
        patient_weight,
        patient_birth_date,
        body_part_examined,
        study_instance_uid,
        images,
        total_images,
        total_size,
        modalities
      } = patient;

      const clinical_notes = clinicalNotesMap.get(patient_id) || patient.clinical_notes || "";

      // === MySQL: Insert patient ===
      await db.execute(
        `REPLACE INTO medical_patients 
         (patient_id, patient_name, clinical_notes, total_images, total_size, modalities,
          patient_sex, patient_age, patient_size, patient_weight, patient_birth_date, 
          body_part_examined, study_instance_uid) 
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          patient_id,
          patient_name,
          clinical_notes,
          total_images,
          total_size,
          JSON.stringify(modalities),
          patient_sex || "",
          patient_age || "",
          patient_size || "",
          patient_weight || "",
          patient_birth_date || "",
          body_part_examined || "",
          study_instance_uid || ""
        ]
      );

      // === Redis: Store patient ===
      await redisClient.hSet(`medical:patient:${patient_id}`, {
        patient_id: String(patient_id),
        patient_name: patient_name || "",
        clinical_notes: clinical_notes,
        total_images: String(total_images),
        total_size: String(total_size),
        modalities: JSON.stringify(modalities),
        patient_sex: patient_sex || "",
        patient_age: patient_age || "",
        patient_size: patient_size || "",
        patient_weight: patient_weight || "",
        patient_birth_date: patient_birth_date || "",
        body_part_examined: body_part_examined || "",
        study_instance_uid: study_instance_uid || ""
      });

      // === Insert images and metadata ===
      for (let j = 0; j < images.length; j++) {
        const img = images[j];
        totalImages++;

        // MySQL: Insert image
        await db.execute(
          `INSERT INTO medical_images 
           (patient_id, file_path, file_name, file_size, modality, 
            instance_number, series_number, acquisition_number, created_at) 
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          [
            patient_id,
            img.file_path,
            img.file_name,
            img.file_size,
            img.modality,
            img.instance_number || "",
            img.series_number || "",
            img.acquisition_number || "",
            img.created_at
          ]
        );

        // Redis: Store image
        await redisClient.hSet(`medical:image:${totalImages}`, {
          image_id: String(totalImages),
          patient_id: String(patient_id),
          file_path: img.file_path,
          file_name: img.file_name,
          file_size: String(img.file_size),
          modality: img.modality,
          instance_number: img.instance_number || "",
          series_number: img.series_number || "",
          acquisition_number: img.acquisition_number || ""
        });

        // Add to patient's image set
        await redisClient.sAdd(`medical:patient:${patient_id}:images`, String(totalImages));

        // Store metadata
        if (img.metadata && Object.keys(img.metadata).length > 0) {
          const meta = img.metadata;


          try {
            const values = [
              patient_id,
              j,
              img.file_path,
              meta.SOPClassUID || "",
              meta.SOPInstanceUID || "",
              meta.StudyInstanceUID || "",
              meta.SeriesInstanceUID || "",
              meta.FrameOfReferenceUID || "",
              meta.StudyDate || "",
              meta.StudyTime || "",
              meta.SeriesDate || "",
              meta.SeriesTime || "",
              meta.AcquisitionDate || "",
              meta.AcquisitionTime || "",
              meta.ContentDate || "",
              meta.ContentTime || "",
              meta.AccessionNumber || "",
              meta.StudyID || "",
              meta.SeriesNumber || "",
              meta.AcquisitionNumber || "",
              meta.InstanceNumber || "",
              meta.Modality || "",
              meta.Manufacturer || "",
              meta.InstitutionName || "",
              meta.ManufacturerModelName || "",
              meta.SoftwareVersions || "",
              meta.PatientPosition || "",
              meta.BodyPartExamined || "",
              meta.ScanningSequence || "",
              meta.SequenceVariant || "",
              meta.ScanOptions || "",
              meta.MRAcquisitionType || "",
              meta.SequenceName || "",
              meta.SliceThickness || "",
              meta.SpacingBetweenSlices || "",
              meta.RepetitionTime || "",
              meta.EchoTime || "",
              meta.EchoNumbers || "",
              meta.NumberOfAverages || "",
              meta.ImagingFrequency || "",
              meta.ImagedNucleus || "",
              meta.MagneticFieldStrength || "",
              meta.NumberOfPhaseEncodingSteps || "",
              meta.EchoTrainLength || "",
              meta.PercentSampling || "",
              meta.PercentPhaseFieldOfView || "",
              meta.PixelBandwidth || "",
              meta.FlipAngle || "",
              meta.VariableFlipAngleFlag || "",
              meta.SAR || "",
              meta.dBdt || "",
              meta.ImagePositionPatient || "",
              meta.ImageOrientationPatient || "",
              meta.SliceLocation || "",
              meta.PositionReferenceIndicator || "",
              meta.SamplesPerPixel || "",
              meta.PhotometricInterpretation || "",
              meta.Rows || "",
              meta.Columns || "",
              meta.PixelSpacing || "",
              meta.BitsAllocated || "",
              meta.BitsStored || "",
              meta.HighBit || "",
              meta.PixelRepresentation || "",
              meta.SmallestImagePixelValue || "",
              meta.LargestImagePixelValue || "",
              meta.WindowCenter || "",
              meta.WindowWidth || "",
              meta.WindowCenterWidthExplanation || "",
              meta.AcquisitionMatrix || "",
              meta.InPlanePhaseEncodingDirection || "",
              meta.TransmitCoilName || "",
              meta.StudyDescription || "",
              meta.SeriesDescription || "",
              meta.RequestedProcedureDescription || "",
              meta.PerformedProcedureStepStartDate || "",
              meta.PerformedProcedureStepStartTime || "",
              meta.PerformedProcedureStepID || "",
              meta.PerformedProcedureStepDescription || "",
              meta.CommentsOnThePerformedProcedureStep || "",
              meta.ReferringPhysicianName || "",
              meta.PerformingPhysicianName || "",
              meta.SpecificCharacterSet || "",
              meta.PatientIdentityRemoved || "",
              meta.DeidentificationMethod || "",
              meta.ImageType || "",
              meta.AngioFlag || "",
              meta.InstanceCreationDate || "",
              meta.InstanceCreationTime || "",
              JSON.stringify(meta)
            ];


            // Câu SQL với đúng 90 placeholders
            await db.execute(
              `INSERT INTO medical_metadata (
            patient_id, image_index, file_path,
            sop_class_uid, sop_instance_uid, study_instance_uid, series_instance_uid, frame_of_reference_uid,
            study_date, study_time, series_date, series_time, acquisition_date, acquisition_time, 
            content_date, content_time, accession_number, study_id, series_number, acquisition_number, instance_number,
            modality, manufacturer, institution_name, manufacturer_model_name, software_versions,
            patient_position, body_part_examined,
            scanning_sequence, sequence_variant, scan_options, mr_acquisition_type, sequence_name,
            slice_thickness, spacing_between_slices, repetition_time, echo_time, echo_numbers,
            number_of_averages, imaging_frequency, imaged_nucleus, magnetic_field_strength,
            number_of_phase_encoding_steps, echo_train_length, percent_sampling, percent_phase_field_of_view,
            pixel_bandwidth, flip_angle, variable_flip_angle_flag, sar, db_dt,
            image_position_patient, image_orientation_patient, slice_location, position_reference_indicator,
            samples_per_pixel, photometric_interpretation, image_rows, image_columns, pixel_spacing,
            bits_allocated, bits_stored, high_bit, pixel_representation,
            smallest_image_pixel_value, largest_image_pixel_value, window_center, window_width,
            window_center_width_explanation, acquisition_matrix, in_plane_phase_encoding_direction,
            transmit_coil_name, study_description, series_description, requested_procedure_description,
            performed_procedure_step_start_date, performed_procedure_step_start_time, 
            performed_procedure_step_id, performed_procedure_step_description, comments_on_performed_procedure_step,
            referring_physician_name, performing_physician_name, specific_character_set,
            patient_identity_removed, deidentification_method, image_type, angio_flag,
            instance_creation_date, instance_creation_time, metadata_json
          ) VALUES (
            ${Array(90).fill("?").join(",")}
          )`,
              values
            );

          } catch (err) {
            console.error(`Error inserting MySQL metadata: ${err.message}`);
          }

          // Redis: Store metadata as HASH with all fields
          const metadataKey = `metadata:${patient_id}:${j}`;
          try {
            const redisMetadata = {
              patient_id: String(patient_id),
              image_index: String(j),
              file_path: img.file_path,

              // Convert all metadata fields to strings
              sop_class_uid: meta.SOPClassUID || "",
              sop_instance_uid: meta.SOPInstanceUID || "",
              study_instance_uid: meta.StudyInstanceUID || "",
              series_instance_uid: meta.SeriesInstanceUID || "",
              frame_of_reference_uid: meta.FrameOfReferenceUID || "",

              study_date: meta.StudyDate || "",
              study_time: meta.StudyTime || "",
              series_date: meta.SeriesDate || "",
              series_time: meta.SeriesTime || "",
              acquisition_date: meta.AcquisitionDate || "",
              acquisition_time: meta.AcquisitionTime || "",
              content_date: meta.ContentDate || "",
              content_time: meta.ContentTime || "",
              accession_number: meta.AccessionNumber || "",
              study_id: meta.StudyID || "",
              series_number: meta.SeriesNumber || "",
              acquisition_number: meta.AcquisitionNumber || "",
              instance_number: meta.InstanceNumber || "",

              modality: meta.Modality || "",
              manufacturer: meta.Manufacturer || "",
              institution_name: meta.InstitutionName || "",
              manufacturer_model_name: meta.ManufacturerModelName || "",
              software_versions: meta.SoftwareVersions || "",

              patient_position: meta.PatientPosition || "",
              body_part_examined: meta.BodyPartExamined || "",

              scanning_sequence: meta.ScanningSequence || "",
              sequence_variant: meta.SequenceVariant || "",
              scan_options: meta.ScanOptions || "",
              mr_acquisition_type: meta.MRAcquisitionType || "",
              sequence_name: meta.SequenceName || "",
              slice_thickness: meta.SliceThickness || "",
              spacing_between_slices: meta.SpacingBetweenSlices || "",
              repetition_time: meta.RepetitionTime || "",
              echo_time: meta.EchoTime || "",
              echo_numbers: meta.EchoNumbers || "",
              number_of_averages: meta.NumberOfAverages || "",
              imaging_frequency: meta.ImagingFrequency || "",
              imaged_nucleus: meta.ImagedNucleus || "",
              magnetic_field_strength: meta.MagneticFieldStrength || "",
              number_of_phase_encoding_steps: meta.NumberOfPhaseEncodingSteps || "",
              echo_train_length: meta.EchoTrainLength || "",
              percent_sampling: meta.PercentSampling || "",
              percent_phase_field_of_view: meta.PercentPhaseFieldOfView || "",
              pixel_bandwidth: meta.PixelBandwidth || "",
              flip_angle: meta.FlipAngle || "",
              variable_flip_angle_flag: meta.VariableFlipAngleFlag || "",
              sar: meta.SAR || "",
              db_dt: meta.dBdt || "",

              image_position_patient: meta.ImagePositionPatient || "",
              image_orientation_patient: meta.ImageOrientationPatient || "",
              slice_location: meta.SliceLocation || "",
              position_reference_indicator: meta.PositionReferenceIndicator || "",

              samples_per_pixel: meta.SamplesPerPixel || "",
              photometric_interpretation: meta.PhotometricInterpretation || "",
              rows: meta.Rows || "",
              columns: meta.Columns || "",
              pixel_spacing: meta.PixelSpacing || "",
              bits_allocated: meta.BitsAllocated || "",
              bits_stored: meta.BitsStored || "",
              high_bit: meta.HighBit || "",
              pixel_representation: meta.PixelRepresentation || "",
              smallest_image_pixel_value: meta.SmallestImagePixelValue || "",
              largest_image_pixel_value: meta.LargestImagePixelValue || "",
              window_center: meta.WindowCenter || "",
              window_width: meta.WindowWidth || "",
              window_center_width_explanation: meta.WindowCenterWidthExplanation || "",

              acquisition_matrix: meta.AcquisitionMatrix || "",
              in_plane_phase_encoding_direction: meta.InPlanePhaseEncodingDirection || "",
              transmit_coil_name: meta.TransmitCoilName || "",

              study_description: meta.StudyDescription || "",
              series_description: meta.SeriesDescription || "",
              requested_procedure_description: meta.RequestedProcedureDescription || "",
              performed_procedure_step_start_date: meta.PerformedProcedureStepStartDate || "",
              performed_procedure_step_start_time: meta.PerformedProcedureStepStartTime || "",
              performed_procedure_step_id: meta.PerformedProcedureStepID || "",
              performed_procedure_step_description: meta.PerformedProcedureStepDescription || "",
              comments_on_performed_procedure_step: meta.CommentsOnThePerformedProcedureStep || "",

              referring_physician_name: meta.ReferringPhysicianName || "",
              performing_physician_name: meta.PerformingPhysicianName || "",

              specific_character_set: meta.SpecificCharacterSet || "",
              patient_identity_removed: meta.PatientIdentityRemoved || "",
              deidentification_method: meta.DeidentificationMethod || "",

              image_type: meta.ImageType || "",
              angio_flag: meta.AngioFlag || "",
              instance_creation_date: meta.InstanceCreationDate || "",
              instance_creation_time: meta.InstanceCreationTime || "",

              // Full JSON as backup
              metadata_json: JSON.stringify(meta)
            };

            await redisClient.hSet(metadataKey, redisMetadata);
            totalMetadata++;

            if (totalMetadata <= 3) {
              console.log(`✓ Inserted metadata: ${metadataKey}`);
            }
          } catch (err) {
            console.error(`Error storing Redis metadata: ${err.message}`);
          }
        }
      }

      totalPatients++;

      if (totalPatients % 10 === 0) {
        process.stdout.write(`\rProcessed: ${totalPatients}/${patientData.length} patients (${totalMetadata} metadata)...`);
      }
    }
  }

  console.log();
  console.log(`✓ Inserted ${totalPatients} patients`);
  console.log(`✓ Inserted ${totalImages} images`);
  console.log(`✓ Inserted ${totalMetadata} metadata records`);
  console.log(`✓ Data loaded successfully\n`);
}

// =========================
// MODIFIED initMySQL TO ADD NEW FIELDS
// =========================
async function initMySQL() {
  const db = await mysql.createConnection({
    host: "127.0.0.1",
    user: "root",
    password: "090604",
    database: "dbms",
    port: 3306
  });

  console.log("✓ MySQL connected");

  // // Drop existing tables
  try {
    await db.execute(`DROP TABLE IF EXISTS medical_metadata;`);
    await db.execute(`DROP TABLE IF EXISTS medical_images;`);
    await db.execute(`DROP TABLE IF EXISTS medical_patients;`);
    await db.execute(`DROP TABLE IF EXISTS counter;`);
    console.log("✓ Dropped old tables");
  } catch (err) {
    console.log("No old tables to drop");
  }

  // TABLE: medical_patients (with additional fields)
  await db.execute(`
    CREATE TABLE IF NOT EXISTS medical_patients (
      patient_id INT PRIMARY KEY,
      patient_name VARCHAR(255),
      patient_sex VARCHAR(10),
      patient_age VARCHAR(20),
      patient_size VARCHAR(20),
      patient_weight VARCHAR(20),
      patient_birth_date VARCHAR(20),
      body_part_examined VARCHAR(100),
      study_instance_uid TEXT,
      clinical_notes TEXT,
      total_images INT DEFAULT 0,
      total_size BIGINT DEFAULT 0,
      modalities TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_patient_name (patient_name),
      INDEX idx_body_part (body_part_examined),
      FULLTEXT(clinical_notes)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);
  console.log("✓ Created medical_patients table");

  // TABLE: medical_images
  await db.execute(`
    CREATE TABLE IF NOT EXISTS medical_images (
      image_id INT AUTO_INCREMENT PRIMARY KEY,
      patient_id INT,
      file_path TEXT,
      file_name VARCHAR(255),
      file_size BIGINT,
      modality VARCHAR(50),
      instance_number VARCHAR(20),
      series_number VARCHAR(20),
      acquisition_number VARCHAR(20),
      created_at TIMESTAMP,
      INDEX idx_patient_id (patient_id),
      INDEX idx_modality (modality),
      INDEX idx_series (series_number),
      FOREIGN KEY (patient_id) REFERENCES medical_patients(patient_id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);
  console.log("✓ Created medical_images table");

  // TABLE: medical_metadata
  await db.execute(`
    CREATE TABLE IF NOT EXISTS medical_metadata (
      metadata_id INT AUTO_INCREMENT PRIMARY KEY,
      patient_id INT,
      image_index INT,
      file_path TEXT,
      
      -- DICOM Core Fields
      sop_class_uid TEXT,
      sop_instance_uid TEXT,
      study_instance_uid TEXT,
      series_instance_uid TEXT,
      frame_of_reference_uid TEXT,
      
      -- Study/Series Information
      study_date VARCHAR(20),
      study_time VARCHAR(50),
      series_date VARCHAR(20),
      series_time VARCHAR(50),
      acquisition_date VARCHAR(20),
      acquisition_time VARCHAR(50),
      content_date VARCHAR(20),
      content_time VARCHAR(50),
      accession_number VARCHAR(100),
      study_id VARCHAR(100),
      series_number VARCHAR(20),
      acquisition_number VARCHAR(20),
      instance_number VARCHAR(20),
      
      -- Equipment Information
      modality VARCHAR(10),
      manufacturer VARCHAR(100),
      institution_name VARCHAR(255),
      manufacturer_model_name VARCHAR(100),
      software_versions VARCHAR(100),
      
      -- Patient Information (from image)
      patient_position VARCHAR(20),
      body_part_examined VARCHAR(50),
      
      -- Imaging Parameters
      scanning_sequence VARCHAR(50),
      sequence_variant VARCHAR(100),
      scan_options VARCHAR(100),
      mr_acquisition_type VARCHAR(10),
      sequence_name VARCHAR(100),
      slice_thickness VARCHAR(20),
      spacing_between_slices VARCHAR(20),
      repetition_time VARCHAR(20),
      echo_time VARCHAR(20),
      echo_numbers VARCHAR(20),
      number_of_averages VARCHAR(20),
      imaging_frequency VARCHAR(50),
      imaged_nucleus VARCHAR(20),
      magnetic_field_strength VARCHAR(20),
      number_of_phase_encoding_steps VARCHAR(20),
      echo_train_length VARCHAR(20),
      percent_sampling VARCHAR(20),
      percent_phase_field_of_view VARCHAR(20),
      pixel_bandwidth VARCHAR(20),
      flip_angle VARCHAR(20),
      variable_flip_angle_flag VARCHAR(10),
      sar VARCHAR(50),
      db_dt VARCHAR(50),
      
      -- Image Position/Orientation
      image_position_patient TEXT,
      image_orientation_patient TEXT,
      slice_location VARCHAR(50),
      position_reference_indicator VARCHAR(100),
      
      -- Image Characteristics
      samples_per_pixel VARCHAR(10),
      photometric_interpretation VARCHAR(50),
      image_rows VARCHAR(10),
      image_columns VARCHAR(10),
      pixel_spacing TEXT,
      bits_allocated VARCHAR(10),
      bits_stored VARCHAR(10),
      high_bit VARCHAR(10),
      pixel_representation VARCHAR(10),
      smallest_image_pixel_value VARCHAR(20),
      largest_image_pixel_value VARCHAR(20),
      window_center VARCHAR(20),
      window_width VARCHAR(20),
      window_center_width_explanation VARCHAR(100),
      
      -- Acquisition Details
      acquisition_matrix TEXT,
      in_plane_phase_encoding_direction VARCHAR(20),
      transmit_coil_name VARCHAR(100),
      
      -- Procedure Information
      study_description TEXT,
      series_description TEXT,
      requested_procedure_description TEXT,
      performed_procedure_step_start_date VARCHAR(20),
      performed_procedure_step_start_time VARCHAR(50),
      performed_procedure_step_id VARCHAR(100),
      performed_procedure_step_description TEXT,
      comments_on_performed_procedure_step TEXT,
      
      -- Physician Information
      referring_physician_name VARCHAR(255),
      performing_physician_name VARCHAR(255),
      
      -- Privacy
      specific_character_set VARCHAR(50),
      patient_identity_removed VARCHAR(10),
      deidentification_method TEXT,
      
      -- Other
      image_type TEXT,
      angio_flag VARCHAR(10),
      instance_creation_date VARCHAR(20),
      instance_creation_time VARCHAR(50),
      
      -- Full JSON backup
      metadata_json JSON,
      
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_patient_id (patient_id),
      INDEX idx_patient_image (patient_id, image_index),
      INDEX idx_modality (modality),
      INDEX idx_series_number (series_number),
      INDEX idx_body_part (body_part_examined),
      FOREIGN KEY (patient_id) REFERENCES medical_patients(patient_id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);
  console.log("✓ Created medical_metadata table");


  // TABLE: counter
  await db.execute(`
    CREATE TABLE IF NOT EXISTS counter (
      id INT PRIMARY KEY,
      count INT DEFAULT 0
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
  `);
  console.log("✓ Created counter table");

  await db.execute(`INSERT IGNORE INTO counter(id, count) VALUES (1, 0);`);
  console.log("✓ Initialized counter\n");

  return db;
}
// =========================
// 6. REDIS CONNECTION
// =========================
async function initRedis() {
  const client = redis.createClient();
  await client.connect();

  // Tắt chặn ghi khi RDB snapshot fail
  await client.configSet('stop-writes-on-bgsave-error', 'no');

  console.log("✓ Redis connected\n");
  return client;
}

// =========================
// 8. DEMO FUNCTIONS
// =========================

// DEMO 1: Query with FULLTEXT search
async function demoFullTextSearch(db, redisClient) {
  console.log("=".repeat(60));
  console.log("DEMO 1: FULLTEXT SEARCH IN CLINICAL NOTES");
  console.log("=".repeat(60));

  const keyword = "stenosis";
  console.log(`Searching for: "${keyword}"\n`);

  // MySQL FULLTEXT
  const mysqlStart = performance.now();
  const [mysqlResult] = await db.query(
    `SELECT patient_id, patient_name, LEFT(clinical_notes, 100) as notes_preview 
     FROM medical_patients 
     WHERE MATCH(clinical_notes) AGAINST(? IN NATURAL LANGUAGE MODE)
     LIMIT 10`,
    [keyword]
  );
  const mysqlTime = (performance.now() - mysqlStart).toFixed(3);

  console.log(`MySQL Time: ${mysqlTime}ms`);
  console.log(`Found: ${mysqlResult.length} patients`);

  // Redis scan and filter
  const redisStart = performance.now();
  const keys = await redisClient.keys("medical:patient:*");
  const matches = [];

  for (let key of keys) {
    if (key.includes(":images")) continue;
    const notes = await redisClient.hGet(key, "clinical_notes");
    if (notes && notes.toLowerCase().includes(keyword.toLowerCase())) {
      const data = await redisClient.hGetAll(key);
      matches.push(data);
    }
  }
  const redisTime = (performance.now() - redisStart).toFixed(3);

  console.log(`\nRedis Time: ${redisTime}ms`);
  console.log(`Found: ${matches.length} patients\n`);

  const speedup = (redisTime / mysqlTime).toFixed(2);
  console.log(`MySQL is ${speedup}x FASTER for full-text search\n`);
}

// DEMO 2: Key lookup
async function demoKeyLookup(db, redisClient) {
  console.log("=".repeat(60));
  console.log("DEMO 2: KEY LOOKUP BY PATIENT ID");
  console.log("=".repeat(60));

  const testId = 1;
  console.log(`Looking up Patient ID: ${testId}\n`);

  // MySQL
  const mysqlStart = performance.now();
  const [patient] = await db.query("SELECT * FROM medical_patients WHERE patient_id=?", [testId]);
  const [images] = await db.query("SELECT COUNT(*) as cnt FROM medical_images WHERE patient_id=?", [testId]);
  const mysqlTime = (performance.now() - mysqlStart).toFixed(3);

  console.log(`MySQL Time: ${mysqlTime}ms`);
  if (patient[0]) {
    console.log(`Patient: ${patient[0].patient_name}, Images: ${images[0].cnt}`);
  }

  // Redis
  const redisStart = performance.now();
  const patientData = await redisClient.hGetAll(`medical:patient:${testId}`);
  const imageIds = await redisClient.sMembers(`medical:patient:${testId}:images`);
  const redisTime = (performance.now() - redisStart).toFixed(3);

  console.log(`\nRedis Time: ${redisTime}ms`);
  if (patientData.patient_name) {
    console.log(`Patient: ${patientData.patient_name}, Images: ${imageIds.length}`);
  }

  const speedup = (mysqlTime / redisTime).toFixed(2);
  console.log(`\nRedis is ${speedup}x FASTER\n`);
}

// DEMO 3: Counter
async function demoCounter(db, redisClient) {
  console.log("=".repeat(60));
  console.log("DEMO 3: ATOMIC COUNTER (100 increments)");
  console.log("=".repeat(60));

  const iterations = 100;

  // MySQL
  const mysqlStart = performance.now();
  for (let i = 0; i < iterations; i++) {
    await db.query("UPDATE counter SET count = count + 1 WHERE id=1");
  }
  const mysqlTime = (performance.now() - mysqlStart).toFixed(3);
  console.log(`MySQL: ${mysqlTime}ms (${(mysqlTime / iterations).toFixed(3)}ms avg)`);

  // Redis
  const redisStart = performance.now();
  for (let i = 0; i < iterations; i++) {
    await redisClient.incr("medical:counter");
  }
  const redisTime = (performance.now() - redisStart).toFixed(3);
  console.log(`Redis: ${redisTime}ms (${(redisTime / iterations).toFixed(3)}ms avg)`);

  const speedup = (mysqlTime / redisTime).toFixed(2);
  console.log(`\nRedis is ${speedup}x FASTER\n`);
}


// DEMO 4: Transaction
async function demoTransaction(db, redisClient) {
  console.log("=".repeat(60));
  console.log("DEMO 4: TRANSACTION SUPPORT");
  console.log("=".repeat(60));

  // MySQL
  console.log("MySQL Transaction:\n");
  try {
    await db.beginTransaction();
    console.log("BEGIN");

    const [before] = await db.query("SELECT clinical_notes FROM medical_patients WHERE patient_id=1");
    console.log(`Before: ${before[0]?.clinical_notes || 'N/A'}`);

    await db.execute("UPDATE medical_patients SET clinical_notes=? WHERE patient_id=1", [`${before[0]?.clinical_notes} (Updated_MySQL)`]);

    const [after] = await db.query("SELECT clinical_notes FROM medical_patients WHERE patient_id=1");
    console.log(`After: ${after[0]?.clinical_notes}`);

    await db.commit();
    console.log("COMMIT successful\n");
  } catch (err) {
    await db.rollback();
    console.log("ROLLBACK");
  }

  // Redis
  console.log("Redis Transaction:\n");
  const tx = redisClient.multi();
  const beforeRedis = await redisClient.hGet("medical:patient:1", "clinical_notes");
  console.log(`Before: ${beforeRedis || 'N/A'}`);

  tx.hSet("medical:patient:1", "clinical_notes", `${beforeRedis} (Updated_Redis)`);
  await tx.exec();

  const afterRedis = await redisClient.hGet("medical:patient:1", "clinical_notes");
  console.log(`After: ${afterRedis}`);
  console.log("EXEC successful\n");
}

// DEMO 5: Transaction Error
async function demoTransactionError(db, redisClient) {
  console.log("=".repeat(60));
  console.log("DEMO 5: TRANSACTION ERROR HANDLING");
  console.log("=".repeat(60));

  // MySQL with rollback
  console.log("MySQL Transaction with Error:\n");
  const [before] = await db.query("SELECT clinical_notes FROM medical_patients WHERE patient_id=1");
  console.log(`Initial: ${before[0]?.clinical_notes}`);

  try {
    await db.beginTransaction();
    await db.execute("UPDATE medical_patients SET clinical_notes=? WHERE patient_id=1", ["Should_Rollback"]);
    console.log("UPDATE executed");

    // Force error
    await db.execute("INSERT INTO medical_patients (patient_id, clinical_notes) VALUES (1, 'duplicate')");
    await db.commit();
  } catch (err) {
    console.log(`Error: ${err.message}`);
    await db.rollback();
    console.log("ROLLBACK executed");
  }

  const [after] = await db.query("SELECT clinical_notes FROM medical_patients WHERE patient_id=1");
  console.log(`Final: ${after[0]?.clinical_notes}\n`);

  // Redis without rollback
  console.log("Redis Transaction with Error:\n");
  const beforeRedis = await redisClient.hGet("medical:patient:1", "clinical_notes");
  console.log(`Initial: ${beforeRedis}`);

  const tx = redisClient.multi();
  tx.hSet("medical:patient:1", "clinical_notes", "Will_Stay");
  tx.incr("medical:patient:1"); // This will fail

  const results = await tx.exec();
  results.forEach((r, i) => console.log(`Step ${i + 1}: ${r instanceof Error ? 'FAILED' : 'SUCCESS'}`));

  const afterRedis = await redisClient.hGet("medical:patient:1", "clinical_notes");
  console.log(`Final: ${afterRedis}\n`);
}

// DEMO 6: Concurrency
async function demoConcurrency(db, redisClient) {
  console.log("=".repeat(60));
  console.log("DEMO 6: CONCURRENCY CONTROL");
  console.log("=".repeat(60));

  // MySQL pessimistic locking
  console.log("MySQL (Row Locking):\n");
  try {
    await db.beginTransaction();
    console.log("BEGIN");

    const [locked] = await db.execute("SELECT * FROM medical_patients WHERE patient_id=1 FOR UPDATE");
    console.log(`Row LOCKED: Patient ${locked[0]?.patient_id}`);
    console.log("Other transactions WAIT\n");

    await db.commit();
    console.log("COMMIT\n");
  } catch (err) {
    await db.rollback();
  }

  // Redis optimistic locking
  console.log("Redis (Optimistic Locking):\n");
  await redisClient.watch("medical:patient:1");
  console.log("WATCH medical:patient:1");

  const name = await redisClient.hGet("medical:patient:1", "patient_name");
  const tx = redisClient.multi();
  tx.hSet("medical:patient:1", "patient_name", name + "_edited");

  const result = await tx.exec();
  console.log(result === null ? "EXEC FAILED (modified by another client)" : "EXEC successful\n");
}


// =========================
// DEMO 7: REAL MYSQL CONCURRENCY (PESSIMISTIC LOCKING)
// =========================
async function demoRealConcurrencyMySQL(db) {
  console.log("=".repeat(60));
  console.log("DEMO 7: REAL MYSQL CONCURRENCY - PESSIMISTIC LOCKING");
  console.log("=".repeat(60));
  console.log("Simulating two clients A & B accessing same patient...\n");

  // Create two separate connections
  const clientA = await mysql.createConnection({
    host: "127.0.0.1",
    user: "root",
    password: "090604",
    database: "dbms",
    port: 3306
  });

  const clientB = await mysql.createConnection({
    host: "127.0.0.1",
    user: "root",
    password: "090604",
    database: "dbms",
    port: 3306
  });

  try {
    // CLIENT A: BEGIN + LOCK ROW
    console.log("CLIENT A: BEGIN TRANSACTION");
    await clientA.beginTransaction();

    const [beforeA] = await clientA.execute(
      "SELECT patient_id, patient_name, clinical_notes FROM medical_patients WHERE patient_id=1 FOR UPDATE"
    );
    console.log(`CLIENT A locked patient: ${beforeA[0].patient_name}`);
    console.log(`Current notes: ${beforeA[0].clinical_notes}`);

    // CLIENT B: TRY TO UPDATE (WILL BLOCK)
    console.log("\nCLIENT B: Attempting UPDATE (will block until A commits)...");
    const startTime = Date.now();

    // This will block until clientA commits
    const clientB_promise = clientB
      .execute(
        "UPDATE medical_patients SET clinical_notes=CONCAT(clinical_notes, ' [UPDATED BY CLIENT B]') WHERE patient_id=1"
      )
      .then(async () => {
        const endTime = Date.now();
        const waitTime = ((endTime - startTime) / 1000).toFixed(2);
        console.log(`\nCLIENT B: UPDATE COMPLETED after waiting ${waitTime} seconds`);

        const [afterB] = await clientB.execute(
          "SELECT clinical_notes FROM medical_patients WHERE patient_id=1"
        );
        console.log(`CLIENT B sees: ${afterB[0].clinical_notes}`);
      })
      .catch(err => {
        console.error("CLIENT B error:", err.message);
      });

    console.log("\nCLIENT A: COMMIT (releasing lock)");
    await clientA.commit();

    // Wait for CLIENT B to complete
    await clientB_promise;

    // Verify final state
    const [finalState] = await clientA.execute(
      "SELECT clinical_notes FROM medical_patients WHERE patient_id=1"
    );
    console.log(`\nFinal state: ${finalState[0].clinical_notes}`);

    console.log("\nMySQL Pessimistic Locking: CLIENT B was BLOCKED until CLIENT A committed!\n");

  } catch (err) {
    console.error("Error in MySQL demo:", err.message);
    await clientA.rollback();
  } finally {
    await clientA.end();
    await clientB.end();
  }
}


// =========================
// DEMO 8: REAL REDIS CONCURRENCY (OPTIMISTIC LOCKING)
// =========================
async function demoRealConcurrencyRedis(redisClient) {
  console.log("=".repeat(60));
  console.log("DEMO 8: REAL REDIS CONCURRENCY - OPTIMISTIC LOCKING");
  console.log("=".repeat(60));
  console.log("Simulating CLIENT A (with WATCH) and CLIENT B (modifier)...\n");

  try {
    // CLIENT A: WATCH + Read value
    console.log("CLIENT A: WATCH medical:patient:1");
    await redisClient.watch("medical:patient:1");

    const oldNotes = await redisClient.hGet("medical:patient:1", "clinical_notes");
    console.log(`CLIENT A reads: ${oldNotes}`);

    // // Simulate processing time
    // console.log("\nCLIENT A: Processing... (1 second)");
    // await new Promise(resolve => setTimeout(resolve, 1000));

    // CLIENT B: Modify value BEFORE A commits
    console.log("\nCLIENT B: Modifying value (NO BLOCKING!)");
    await redisClient.hSet(
      "medical:patient:1",
      "clinical_notes",
      oldNotes + " [MODIFIED BY CLIENT B]"
    );
    console.log("CLIENT B: Modification completed immediately");

    const modifiedValue = await redisClient.hGet("medical:patient:1", "clinical_notes");
    console.log(`Current value: ${modifiedValue}`);

    // CLIENT A: Try to EXEC transaction
    console.log("\nCLIENT A: Preparing MULTI/EXEC...");
    const tx = redisClient.multi();
    tx.hSet("medical:patient:1", "clinical_notes", oldNotes + " [UPDATE BY CLIENT A]");

    const result = await tx.exec();

    if (result === null) {
      console.log("CLIENT A: EXEC FAILED - Key was modified by another client!");
      console.log("CLIENT A must RETRY the transaction\n");

      // RETRY logic
      console.log("CLIENT A: RETRYING transaction...");
      const newValue = await redisClient.hGet("medical:patient:1", "clinical_notes");
      await redisClient.hSet(
        "medical:patient:1",
        "clinical_notes",
        newValue + " [UPDATE BY CLIENT A - RETRY]"
      );
      console.log("CLIENT A: RETRY SUCCESSFUL");
    } else {
      console.log("CLIENT A: EXEC SUCCESS (no conflicts)");
    }

    const finalValue = await redisClient.hGet("medical:patient:1", "clinical_notes");
    console.log(`\nFinal value: ${finalValue}`);

    console.log("\nRedis Optimistic Locking: CLIENT B was NOT BLOCKED, CLIENT A detected conflict and RETRIED!\n");

  } catch (err) {
    console.error("Error in Redis demo:", err.message);
  }
}

// =========================
// MENU
// =========================
function ask(question) {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });
  return new Promise(resolve => rl.question(question, ans => { rl.close(); resolve(ans); }));
}

async function showMenu() {
  console.log("=".repeat(60));
  console.log("MEDICAL DATA COMPARISON: Redis vs MySQL");
  console.log("=".repeat(60));
  console.log("1. FullText Search (Clinical Notes)");
  console.log("2. Key Lookup (Patient by ID)");
  console.log("3. Atomic Counter");
  console.log("4. Transaction Success");
  console.log("5. Transaction Error Handling");
  console.log("6. Concurrency Control (Simple Demo)");
  console.log("7. Real MySQL Concurrency (Pessimistic Locking)");
  console.log("8. Real Redis Concurrency (Optimistic Locking)");
  console.log("0. Exit");
  console.log("=".repeat(60));
  return (await ask("Choose: ")).trim();
}

// =========================
// MAIN
// =========================
async function main() {
  try {
    console.log("=".repeat(60));
    console.log("MEDICAL DATA LOADER FROM JSON");
    console.log("=".repeat(60));

    // Read metadata from JSON files in data/metadata
    const metadataArray = readMetadataFromJSON("./data");

    if (metadataArray.length === 0) {
      console.log("⚠ No metadata found in data/metadata folder!");
      process.exit(1);
    }

    // Process into patient structure
    const patientData = processMetadataIntoPatients(metadataArray);

    // Initialize databases
    const db = await initMySQL();
    const redisClient = await initRedis();

    // Insert data
    await insertData(db, redisClient, patientData);

    // Run demos
    while (true) {
      const choice = await showMenu();

      if (choice === "1") await demoFullTextSearch(db, redisClient);
      else if (choice === "2") await demoKeyLookup(db, redisClient);
      else if (choice === "3") await demoCounter(db, redisClient);
      else if (choice === "4") await demoTransaction(db, redisClient);
      else if (choice === "5") await demoTransactionError(db, redisClient);
      else if (choice === "6") await demoConcurrency(db, redisClient);
      else if (choice === "7") await demoRealConcurrencyMySQL(db);
      else if (choice === "8") await demoRealConcurrencyRedis(redisClient);
      else if (choice === "0") {
        console.log("\nExiting...");
        break;
      }
    }


    await db.end();
    await redisClient.quit();
    process.exit(0);

  } catch (error) {
    console.error("Error:", error);
    console.error("Stack trace:", error.stack);
    process.exit(1);
  }
}


main();