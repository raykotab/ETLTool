<?php

namespace tests\ETLTest;

use DateTime;
use PDO;
use PHPUnit\Framework\TestCase;

class ETLTest extends TestCase
{
    /** @var PDO $source1 */
    private $source1;
    /** @var PDO $source2 */
    private $source2;
    /** @var PDO $source3 */
    private $source3;
    /** @var PDO $target1 */
    private $target1;

    public function setUp(): void
    {
        $this->source1 = new PDO('mysql:host=localhost;dbname=test_car', 'root', '');
        $this->source2 = new PDO('mysql:host=localhost;dbname=test_house', 'root', '');
        $this->source3 = new PDO('mysql:host=localhost;dbname=test_job', 'root', '');

        $this->target1 = new PDO('mysql:host=localhost;dbname=test_result', 'root', '');

        // Insert test data into source databases
        $this->source1->query('INSERT INTO test_car (firstname, lastname, zipcode, dateofbirth, gender, registrationdate) VALUES ("testValue", "testValue", "1234 xz", "01/01/88" , "m", "01/01/2021 08:01:00")');
        $this->source2->query('INSERT INTO test_house (firstname, lastname, zipcode, dateofbirth, gender, registrationdate) VALUES ("testValue", "testValue", "1234 xz", "01/01/1988" , "male", "01/02/21 08:03:00")');
        $this->source3->query('INSERT INTO test_job (firstname, lastname, zipcode, dateofbirth, gender, registrationdate) VALUES ("testValue", "testValue", "1234 xz", "June 08 1970" , "mr", "March 01 2021 10:55:00")');
    }

    public function testETL(): void
    {
        // Extract data from source databases
        $data1 = $this->source1->query('SELECT * FROM test_car')->fetchAll(PDO::FETCH_ASSOC);
        $data2 = $this->source2->query('SELECT * FROM test_house')->fetchAll(PDO::FETCH_ASSOC);
        $data3 = $this->source3->query('SELECT * FROM test_job')->fetchAll(PDO::FETCH_ASSOC);

        // Transform data
        $transformedData = [];
        foreach ($data1 as $row) {
            $transformedData[] = [
                'firstname' => \ucfirst($row['firstname']),
                'lastname' => \ucfirst($row['lastname']),
                'zipcode' => \preg_replace('/[\s"]+/', '', $row['zipcode']),
                'dateofbirth' => $this->parseDate($row['dateofbirth']),
                'gender' => $this->unificateGender($row['gender']),
                'registrationdate' => $this->parseDateTime($row['registrationdate']),
                'source' => 'test_car'
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
                'source' => 'test_house'
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
                'source' => 'test_job'
            ];
        }
        // Load data into target database
        foreach ($transformedData as $row) {
            $stmt = $this->target1->prepare('INSERT INTO test_result (firstname, lastname, zipcode, dateofbirth, gender, registrationdate, source) VALUES (:firstname, :lastname, :zipcode, :dateofbirth, :gender, :registrationdate, :source)');
            $stmt->execute($row);
        }

        // Test the results
        $result = $this->target1->query('SELECT * FROM test_result')->fetchAll(PDO::FETCH_ASSOC);
        // $this->assertCount(3, $result);
        $this->assertSame('TestValue', $result[0]['firstname']);
        $this->assertSame('TestValue', $result[1]['firstname']);
        $this->assertSame('TestValue', $result[2]['firstname']);
        $this->assertFalse(\strpos($result[0]['zipcode'], " "));
        $this->assertMatchesRegularExpression(
            '/^\d{4}-\d{2}-\d{2}$/',
            $result[0]['dateofbirth']
        );
        $this->assertSame("M", $result[0]['gender']);
        $this->assertMatchesRegularExpression(
            '/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/',
            $result[0]['registrationdate']
        );
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
