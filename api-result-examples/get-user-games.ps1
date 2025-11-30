$player = curl -X GET "https://open-api.bser.io/v1/user/nickname?query=Philmist" -H  "accept: application/json" -H  "x-api-key: $Env:ER_DEV_APIKEY" | ConvertFrom-Json
$player_hash = $player.user.userId
curl -X GET "https://open-api.bser.io/v1/user/games/uid/$player_hash" -H  "accept: application/json" -H  "x-api-key: $Env:ER_DEV_APIKEY" > user-games.json
