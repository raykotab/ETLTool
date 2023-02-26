<?php

namespace src\ETLModule;

use PDO;
use DateTime;

class ETLModule
{
    /** @var PDO $source1 */
    private $source1;
    /** @var PDO $source2 */
    private $source2;
    /** @var PDO $source3 */
    private $source3;
    /** @var PDO $target1 */
    private $target1;
    /** @var PDO $target2 */
    private $target2;
    /** @var PDO $target3 */
    private $target3;

    public function setConnections(): void
    {
        $this->source1 = new PDO('mysql:host=localhost;dbname=car', 'root', '');
        $this->source2 = new PDO('mysql:host=localhost;dbname=house', 'root', '');
        $this->source3 = new PDO('mysql:host=localhost;dbname=job', 'root', '');

        $this->target1 = new PDO('mysql:host=localhost;dbname=la_car', 'root', '');
        $this->target2 = new PDO('mysql:host=localhost;dbname=la_house', 'root', '');
        $this->target3 = new PDO('mysql:host=localhost;dbname=la_job', 'root', '');
    }

    /**
     * Function that makes the three steps of ETL with all three databases
     * 
     * @return void 
     */
    public function extractTransformLoad(): void
    {
        // Extract data from source databases
        $data1 = $this->source1->query('SELECT * FROM car')->fetchAll(PDO::FETCH_ASSOC);
        $data2 = $this->source2->query('SELECT * FROM house')->fetchAll(PDO::FETCH_ASSOC);
        $data3 = $this->source3->query('SELECT * FROM job')->fetchAll(PDO::FETCH_ASSOC);

        // Transform data
        $transformedData1 = $this->transformData($data1, "car");
        $transformedData2 = $this->transformData($data2, "house");
        $transformedData3 = $this->transformData($data3, "job");

        //load Data
        $this->loadData($transformedData1, $this->target1, "la_car");
        $this->loadData($transformedData2, $this->target2, "la_house");
        $this->loadData($transformedData3, $this->target3, "la_job");
    }

    /**
     * Ends the connections to the databases
     * 
     * @return void
     */
    public function closeConnections(): void
    {
        $this->target1 = null;
        $this->target2 = null;
        $this->target3 = null;
        $this->source1 = null;
        $this->source2 = null;
        $this->source3 = null;
    }

    /**
     * Function for transforming the data into determined formats
     * 
     * @param array $data with different formats
     * @param array $source table name
     * @return array $data with unified formats
     */
    private function transformData(array $data, string $source): array
    {
        /** @var array $transformedData */
        $transformedData = [];

        foreach ($data as $row) {
            $transformedData[] = [
                'firstname' => \ucfirst($row['firstname']),
                'lastname' => \ucwords(preg_replace('/"/', '', $row['lastname'])),
                'zipcode' => \preg_replace('/[\s"]+/', '', $row['zipcode']),
                'dateofbirth' => $this->parseDate($row['dateofbirth']),
                'gender' => $this->unificateGender($row['gender']),
                'registrationdate' => $this->parseDateTime($row['registrationdate']),
                'source' => $source
            ];
        }

        return $transformedData;
    }

    /**
     * Function for taking different gender formats and returning only M or F
     * 
     * @param string $gender
     * @return string Capìtal letter  
     */
    private function unificateGender(string $gender): string
    {
        if ($gender === 'm' || $gender === 'mr' || $gender === 'male') {
            $gender = 'M';
        } else {
            $gender = 'F';
        }

        return $gender;
    }

    /**
     * Function that accepts diferent formats for date string
     * and returns Y-m-d
     * 
     * @param string $dateTimeString The date-time string to parse.
     * @return DateTime The date in the format Y-m-d.
     */
    private function parseDate(string $dateString): string
    {
        $cleanDateString = preg_replace('/"/', '', $dateString);
        /** @var Date|null $dateTiemObject */
        $dateObject = null;
        $formats = array('d/m/y', 'd/m/Y', 'F d Y');
        foreach ($formats as $format) {
            $dateObject = DateTime::createFromFormat($format, $cleanDateString);
            if ($dateObject !== false) {
                break;
            }
        }

        return isset($dateObject) ? $dateObject->format('Y-m-d') : null;
    }

    /**
     * Function that accepts diferent formats for dateTime string
     * and returns Y-m-d H:i:s
     * 
     * @param string $dateTimeString The date-time string to parse.
     * @return string|null The date and time in the format Y-m-d H:i:s, or null if the input could not be parsed.
     */
    private function parseDateTime(string $dateTimeString): ?string
    {
        $cleanDateTimeString =  preg_replace('/"/', '', $dateTimeString);
        /** @var DateTime|null $dateTiemObject */
        $dateTimeObject = null;
        $dateFormats = array('d/m/y', 'd/m/Y', 'F d Y');
        $timeFormats = array('H:i:s', 'H:i', 'g:i A');
        foreach ($dateFormats as $dateFormat) {
            foreach ($timeFormats as $timeFormat) {
                $format = $dateFormat . ' ' . $timeFormat;
                $dateTimeObject = DateTime::createFromFormat($format, $cleanDateTimeString);
                if ($dateTimeObject !== false) {
                    break 2;
                }
            }
        }

        return isset($dateTimeObject) ? $dateTimeObject->format('Y-m-d H:i:s') : null;
    }

    /**
     * Function that loads the data into the new databases.
     * 
     * @param array $transformedData formatted.
     * @param PDO $target connection to load into.
     * @param string $tableName.
     * @return void
     */
    private function loadData(array $transformedData, PDO $target, string $tableName): void
    {
        foreach ($transformedData as $row) {
            $stmt = $target->prepare(
                "INSERT INTO $tableName (firstname, lastname, zipcode, dateofbirth, gender, registrationdate, source)
                VALUES (:firstname, :lastname, :zipcode, :dateofbirth, :gender, :registrationdate, :source)"
            );
            $stmt->execute($row);
        }
    }
}

$etlModule = new ETLModule();
$etlModule->setConnections();
$etlModule->extractTransformLoad();
$etlModule->closeConnections();
