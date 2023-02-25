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
    /** @var PDO $target1 */
    private $target2;
    /** @var PDO $target1 */
    private $target3;

    public function setUp(): void
    {
        $this->source1 = new PDO('mysql:host=localhost;dbname=car', 'root', '');
        $this->source2 = new PDO('mysql:host=localhost;dbname=house', 'root', '');
        $this->source3 = new PDO('mysql:host=localhost;dbname=job', 'root', '');

        $this->target1 = new PDO('mysql:host=localhost;dbname=la_car', 'root', '');
        $this->target2 = new PDO('mysql:host=localhost;dbname=la_house', 'root', '');
        $this->target3 = new PDO('mysql:host=localhost;dbname=la_job', 'root', '');
    }

    public function testETL(): void
    {
        // Extract data from source databases
        $data1 = $this->source1->query('SELECT * FROM car')->fetchAll(PDO::FETCH_ASSOC);
        $data2 = $this->source2->query('SELECT * FROM house')->fetchAll(PDO::FETCH_ASSOC);
        $data3 = $this->source3->query('SELECT * FROM job')->fetchAll(PDO::FETCH_ASSOC);

        // Transform data
        $transformedDataCar = [];
        foreach ($data1 as $row) {
            $transformedDataCar[] = [
                'firstname' => \ucfirst($row['firstname']),
                'lastname' => \ucfirst($row['lastname']),
                'zipcode' => \preg_replace('/[\s"]+/', '', $row['zipcode']),
                'dateofbirth' => $this->parseDate($row['dateofbirth']),
                'gender' => $this->unificateGender($row['gender']),
                'registrationdate' => $this->parseDateTime($row['registrationdate']),
                'source' => 'car'
            ];
        }

        foreach ($data2 as $row) {
            $transformedData[] = [
                'firstname' => \ucfirst($row['firstname']),
                'lastname' => \ucfirst($row['lastname']),
                'zipcode' => \preg_replace('/[\s"]+/', '', $row['zipcode']),
                'dateofbirth' => $this->parseDate($row['dateofbirth']),
                'gender' => $this->unificateGender($row['gender']),
                'registrationdate' => $this->parseDateTime($row['registrationdate']),
                'source' => 'house'
            ];
        }
        foreach ($data3 as $row) {
            $transformedData[] = [
                'firstname' => \ucfirst($row['firstname']),
                'lastname' => \ucfirst($row['lastname']),
                'zipcode' => \preg_replace('/[\s"]+/', '', $row['zipcode']),
                'dateofbirth' => $this->parseDate($row['dateofbirth']),
                'gender' => $this->unificateGender($row['gender']),
                'registrationdate' => $this->parseDateTime($row['registrationdate']),
                'source' => 'job'
            ];
        }
        // Load data into target database
        foreach ($transformedData as $row) {
            $stmt = $this->target1->prepare('INSERT INTO test_result (firstname, lastname, zipcode, dateofbirth, gender, registrationdate, source) VALUES (:firstname, :lastname, :zipcode, :dateofbirth, :gender, :registrationdate, :source)');
            $stmt->execute($row);
        }
    }

    public function tearDown(): void
    {
        // Delete the created rows
        // $stmt = $this->target1->prepare("DELETE FROM test_result WHERE firstname = 'TestValue'");
        // $stmt->execute();
        $stmt1 =  $this->source1->prepare("DELETE FROM test_car WHERE firstname = 'TestValue'");
        $stmt1->execute();
        $stmt2 =  $this->source2->prepare("DELETE FROM test_house WHERE firstname = 'TestValue'");
        $stmt2->execute();
        $stmt3 =  $this->source3->prepare("DELETE FROM test_job WHERE firstname = 'TestValue'");
        $stmt3->execute();

        // Close the database connection
        $this->target1 = null;
        $this->source1 = null;
        $this->source2 = null;
        $this->source3 = null;
    }

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
        $dateObject = null;

        $formats = array('d/m/y', 'd/m/Y', 'F d Y');
        foreach ($formats as $format) {
            $dateObject = DateTime::createFromFormat($format, $dateString);
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
    function parseDateTime(string $dateTimeString): ?string
    {
        $dateTimeObject = null;
        $dateFormats = array('d/m/y', 'd/m/Y', 'F d Y');
        $timeFormats = array('H:i:s', 'H:i', 'g:i A');
        foreach ($dateFormats as $dateFormat) {
            foreach ($timeFormats as $timeFormat) {
                $format = $dateFormat . ' ' . $timeFormat;
                $dateTimeObject = DateTime::createFromFormat($format, $dateTimeString);
                if ($dateTimeObject !== false) {
                    break 2;
                }
            }
        }

        return isset($dateTimeObject) ? $dateTimeObject->format('Y-m-d H:i:s') : null;
    }
}
