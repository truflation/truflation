const { Connection, PublicKey } = require("@_koi/web3.js");
const fs = require("fs");
const path = require("path");
const axios = require("axios");

async function getTaskData(taskID, round) {
  const connection = new Connection("https://testnet.koii.live");

  // Check if TASK_ID is defined
  if (!taskID) {
    throw new Error("TASK_ID is not defined");
  }

  let maxRound;
  let submissionList;
  let taskState;
  let submissionData = {};
  let historicalData = {};

  const accountInfo = await connection.getAccountInfo(new PublicKey(taskID));
  taskState = JSON.parse(accountInfo.data);

  // Create a submissionList to contain each submission_value
  submissionList = [];

  // Identify the round with the highest number
  maxRound = Math.max(...Object.keys(taskState.submissions).map(Number));

  // Iterate through the entries in the highest round
  for (let entry in taskState.submissions[maxRound]) {
    // Extract the submission_value and add it to the list
    submissionList.push(
      taskState.submissions[maxRound][entry].submission_value
    );
  }

  console.log("Submission List: ", submissionList, "at round", maxRound);

  let i = 0;
  while (maxRound >= 0) {

    console.log('maxRound', maxRound)

    let curr_round_data;
    let cid = submissionList[i];

    try {
      curr_round_data = await axios.get(
        `https://${cid}.ipfs.w3s.link/data.json`
      );
    } catch (error) {
      break;
    }

    if (!curr_round_data || !curr_round_data.data) {
      break;
    }
    submissionData[maxRound] = curr_round_data.data;

    console.log('submissionData[maxRound]', submissionData[maxRound])

    // for (let j = 0 ; j < submissionData[maxRound].length ; j ++ ) {
    //   let cid =  submissionData[maxRound][j].cid
    //   try {
    //     historic_data = await axios.get(
    //       `https://${cid}.ipfs.w3s.link/data.json`
    //     );
        
    //   } catch (error) {
    //     break;
    //   }

    //   if (!historic_data || !historic_data.data) {
    //     break;
    //   }
    //   console.log('historic_data.data', historic_data.data)
    //   historicalData[cid] = historic_data.data;
    // }

    maxRound--;
    i++;
  }

  try {
    const filePath = path.join(__dirname, "../output/submissionData.json");
    console.log(`Writing to: ${filePath}`);
    const submissionDataString = JSON.stringify(submissionData, null, 2);

    if (!submissionDataString) {
      console.error("No data to write.");
      return;
    }

    fs.writeFileSync(filePath, submissionDataString);
    console.log("Historical data written to file successfully.");
  } catch (error) {
    console.error("Failed to write historical data to file:", error);
  }

  // try {
  //   const historicFilePath = path.join(__dirname, "../output/historicData.json");
  //   console.log(`Writing to: ${historicFilePath}`);
  //   const historicalDataString = JSON.stringify(historicalData, null, 2);

  //   if (!historicalDataString) {
  //     console.error("No data to write.");
  //     return;
  //   }

  //   fs.writeFileSync(historicFilePath, historicalDataString);
  //   console.log("Historical data written to file successfully.");
  // } catch (error) {
  //   console.error("Failed to write historical data to file:", error);
  // }

  return submissionList;
}

getTaskData("6ENPknrNEhG7kJ8L5Nd1wZdGjN5ypmyVwxUWBGCoCuwo", "0");
// getTaskData('9JWr6aQRjgBFKvrJbp3USEvpgwZqWJzN1gfrSHr76uaz', '0');
