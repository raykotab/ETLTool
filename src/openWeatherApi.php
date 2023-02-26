<?php

$url = "https://archive-api.open-meteo.com/v1/archive?latitude=42.23&longitude=-8.72&start_date=2023-02-07&end_date=2023-02-07&hourly=temperature_2m,rain";

$options = array(
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_HTTPHEADER => array('Content-Type: application/json'),
    CURLOPT_SSL_VERIFYPEER => false
);

$curl = curl_init($url);

curl_setopt_array($curl, $options);

$response = curl_exec($curl);

if(curl_errno($curl)) {
    echo 'Error: ' . curl_error($curl);
}

curl_close($curl);

$data = json_decode($response, true);

$temperatures = $data['hourly']['temperature_2m'];
$rainFall = $data['hourly']['rain'];

$highestTemperatureHour = array_keys($temperatures, max($temperatures))[0];
$highestRainFallHour = array_keys($rainFall, max($rainFall))[0];
echo "Hour with the highest temperature: " . $highestTemperatureHour . "\n";
echo "Hour with the highest rainfall: " . $highestRainFallHour . "\n";
