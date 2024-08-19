# Archive Notice

This repository has been archived and is no longer maintained. It is kept for historical purposes only.

The code is moved to [Truflation](https://github.com/truflation/truflation/tree/dev/koii/examples/koii)

# Old Readme

# K2-Task-Template

Tasks run following a periodic structure of 'rounds':

![Screenshot_20230307-091958](https://user-images.githubusercontent.com/66934242/223565192-3ecce9c6-0f9a-4a58-8b02-2db19c61141f.png)

Each round is set by a specific time period, and nodes participate by uploading data to IPFS, posting CIDs to the K2 settlement layer, and sending messages across REST APIs and WebSockets.

For more information on how the Task Flow works, check out [the runtime environment docs](https://docs.koii.network/develop/microservices-and-tasks/what-are-tasks/gradual-consensus#why-is-it-gradual).

If this is your first time writing a Koii Task, you might want to use the [task organizer](https://www.figma.com/community/file/1220194939977550205/Task-Outline).

## Requirements

- [Node >=16.0.0](https://nodejs.org)
- [Docker compose](https://docs.docker.com/compose/install/docker)

## What's in the template?

`index.js` is the hub of your app, and ties together the other pieces. This will be the entrypoint when your task runs on Task Nodes

`NamespaceWrappers.js` contains the interfaces to make API calls to the core of the task-node. It contains all the necessary functions required to submit and audit the work, as well as the distribution lists

`coreLogic.js` is where you'll define your task, audit, and distribution logic, and controls the majority of task functionality. You can of course break out separate features into sub-files and import them into the core logic before web-packing.

## Runtime Options

There are two ways to run your task when doing development:

1. With GLOBAL_TIMERS="true" (see .env-local)- When the timer is true, IPC calls are made by calculating the average time slots of all the task running your node.

2. With GLOBAL_TIMERS="false" - This allows you to do manual calls to K2 and disables the triggers for round managemnt on K2. Transactions are only accepted during the correct period. Guide for manual calls is in index.js

# Modifying CoreLogic.js

Task nodes will trigger a set of predefined functions during operation.

There are in total 9 functions in CoreLogic which the you can modify according to your needs:

1. _task()_ - The logic for what your task should do goes here. There is a window in round that is dedicated to do work. The code in task is executed in that window.

2. _fetchSubmission()_ - After completing the task , the results/work will be stored somewhere like on IPFS or local levelDB. This function is the place where you can write the logic to fetch that work. It is called in submitTask() function which does the actual submission on K2.

3. _submitTask()_ - It makes the call to namespace function of task-node using the wrapper.

4. _generateDistributionList()_ - You have full freedom to prepare your reward distributions as you like and the logic for that goes here. We have provided a sample logic that rewards 1 KOII to all the needs who did the correct submission for that round. This function is called in submitDistributionList()

5. _submitDistributionList()_ - makes call to the namesapce function of task-node to upload the list and on succesful upload does the transaction to update the state.

6. _validateNode()_ - this function is called to verify the submission value, so based on the value received from the task-state we can vote on the submission.

7. _validateDistribution()_ - The logic to validate the distribution list goes here and the function will receive the distribution list submitted form task-state.

8. _auditTask()_ - makes call to namespace of task-node to raise an audit against the submission value if the validation fails.

9. _auditDistribution()_ - makes call to namespace of task-node to raise an audit against the distribution list if the validation fails.

# Testing and Deploying

## Using unitTest.js

In tests folder, `unitTest.js` file helps you to mock task state parameters that are required in core logic function and test it. Customise the parameters according to your needs and run `node tests/unitTest.js` to test the functions. For example, you can comment out the `coreLogic.task()` function and directly test your fetch submission function.

## Using Jest

To setup test case for entire task execution / individual functions, you can refer the `main.test.js` You can run the tests using : `yarn test`. It will run the test cases and generate a coverage report in `coverage` folder. You might need to customise the `main.test.js` according to your needs. For example, if your main task need more time to execute, you can add the timeout amount:

In line:11 to line:14

```javascript
  it('should performs the core logic task', async () => {
    const result = await coreLogic.task();
    expect(result).not.toContain('ERROR IN EXECUTING TASK');
  }, 100000); // 100000 is the timeout amount in milliseconds
```

## Testing API
To test the API's , you can start your local server using `yarn start` , it will expose the APIs on port `10000`.

## USing Docker

Testing using the docker container should be mostly used for consensus flows, as it will take longer to rebuild and re-deploy the docker container.

## Build

Before deploying a task, you'll need to build it into a single file executable by running
`yarn webpack`

### To get a web3.storage key

If you have already created an account on [web3.storage](https://web3.storage/docs/#quickstart) you'll just need to enter the API key after the prompts in the deploy process. If this is your first time using web3.storage, you'll need to create an account and then create an API key. You can do this by following the instructions [here](https://blog.koii.network/Introduce-web3-storage/).

### Find or create a k2 wallet key

If you have already generated a Koii wallet on yoru filesystem you can obtain the path to it by running `koii config get` which should return something similar to the following:

![截图 2023-03-07 18-13-17](https://user-images.githubusercontent.com/66934242/223565661-ece1591f-2189-4369-8d2a-53393da15834.png)

The `Keypair Path` will be used to pay gas fees and fund your bounty wallet by inputting it into the task CLI.

If you need to create a Koii wallet you can follow the instructions [here](https://docs.koii.network/develop/koii-software-toolkit-sdk/using-the-cli#create-a-koii-wallet). Make sure to either copy your keypair path from the output, or use the method above to supply the task CLI with the proper wallet path.

### Deploy to K2

To test the task with the [K2 Settlement Layer](https://docs.koii.network/develop/settlement-layer/k2-tick-tock-fast-blocks#docusaurus_skipToContent_fallback) you'll need to deploy it.

To publish tasks to the K2 network use `npx @_koii/create-task-cli@latest` if you already installed the package earlier or else use `npm i @_koii/create-task-cli` to get it first . You have two options to create your task using `config-task.yml` and using the `cli`. Check out the sample `config-task.yml` attached in this repo, by default it will look for both `config-task.yml` and `id.json` in your current directory and if not deteched you will have an option to enter your path. Tips on this flow and detailed meaning of each task parameter can be found [in the docs](https://docs.koii.network/develop/koii-software-toolkit-sdk/create-task-cli). One important thing to note is when you're presented with the choice of ARWEAVE, IPFS, or DEVELOPMENT you can select DEVELOPMENT and enter `main` in the following prompt. This will tell the task node to look for a `main.js` file in the `dist` folder. You can create this locally by running `yarn webpack`.

## Run a node locally

If you want to get a closer look at the console and test environment variables, you'll want to use the included docker-compose stack to run a task node locally.

1. Link or copy your wallet into the `config` folder as `id.json`
2. Open `.env-local` and add your TaskID you obtained after deploying to K2 into the `TASKS` environment variable.
3. Run `docker compose up` and watch the output of the `task_node`. You can exit this process when your task has finished, or any other time if you have a long running persistent task.

### Redeploying

You do not need to publish your task every time you make modifications. You do however need to restart the `task_node` in order for the latest code to be used. To prepare your code you can run `yarn webpack` to create the bundle. If you have a `task_node` ruinning already, you can exit it and then run `docker compose up` to restart (or start) the node.

### Environment variables

Open the `.env-local` file and make any modifications you need. You can include environment variables that your task expects to be present here, in case you're using [custom secrets](https://docs.koii.network/develop/microservices-and-tasks/task-development-kit-tdk/using-the-task-namespace/keys-and-secrets).

### API endpoints

By default your API's will be exposed on base URL: http://localhost:8080/task/{TASKID}

You can check out the state of your task using the default API : http://localhost:8080/task/{TASKID}/taskState

`TASKID` is the id that you get when you create your task using `npx`

### Scripts for data extraction and export

1. `fetchAvgData.js` - This script will fetch the average data from the task state for all the rounds and export it to a json file. You can run this script using `node fetchAvgData.js`
2. `subProcessInvocation.py` - This script will invoke the above `fetchAvgData.js` node script in a sub process and can hook post-run of the js script with anything like feeding that json to dataFrame or making json to csv using script given below. You can run this script using `python3 subProcessInvocation.py`. You can also run the `fetchAvgData.js` directly using `node fetchAvgData.js`
3. `csvGeneration.py` - This script will convert the json file (generated by running fetchAvgData.js either directly or through sub-process) to csv file. You can run this script using `python3 csvGeneration.py`

### Example outputs of the scripts

1. For `fetchAvgData.js` - `historicalData.json` file will be generated in the same directory where the script is run. `FindingHistoricalData` file contains the sample output of the script.
2. For `csvGeneration.py` - `data.csv` file will be generated in the same directory where the script is run. `data.csv` file contains the sample output of the script.


