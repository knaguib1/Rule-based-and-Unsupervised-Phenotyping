package edu.cse6250.model

import java.sql.Date

/**
 * @author Hang Su <hangsu@gatech.edu>,
 * @author Yu Jing <yjing43@gatech.edu>,
 */
case class Diagnostic(patientID: String, code: String, date: Date)

case class LabResult(patientID: String, date: Date, testName: String, value: Double)

case class Medication(patientID: String, date: Date, medicine: String)
