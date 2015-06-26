var conf = require('./conf.js'),
    mysql     = require('mysql'),
    util = require('util'),
    MongoClient = require('mongodb').MongoClient;

function Migrator(conf) {
    var self = this;
    this.conf = conf;
    this.tasks = conf.tasks;
    this.mongo_url = 'mongodb://' + conf.mongo.host + ':' + conf.mongo.port + '/' + conf.mongo.database;
    this.mysql_con = mysql.createPool(conf.mysql);
    this.mongo_db = null;

    MongoClient.connect(self.mongo_url, function(err, db) {
        if(!err) {
            self.mongo_db = db;
            self.startNextTask();
        } else {
            throw new Error('Mongo connection failed!');
        }

    });

};

Migrator.prototype.startNextTask = function() {
    switch(this.tasks.splice(0, 1)[0]) {
        case 'rating': this.migrateRating(); break;
        case 'b_rating': this.migrateBRating(); break;
        case 'chat': this.migrateChat(); break;
        case 'history': this.migrateHistory(); break;
        case 'b_history': this.migrateBHistory(); break;
        case 'penalty': this.migratePenalty(); break;
        case 'game_over_test': this.gameOverTest(); break;
        case undefined: util.log("Migration completed, exit."); process.exit(); break;
    }
};

Migrator.prototype.mongoInsert = function(data, collectionName) {
    var self = this;
    var dataStartingLength = data.length;
    var inserted = 0; // счетчик уже вставленных записей
    var col = this.mongo_db.collection(collectionName);
    var dropCollection = typeof this.conf.dropCollection[collectionName] != "undefined" ? this.conf.dropCollection[collectionName] : false; // по умолчанию нужно дропать

    if(dropCollection) {
        col.drop();
        util.log("Collection", collectionName, "dropped.");
        /*
        MongoClient.connect(self.mongo_url, function(err, db) {
            var col = self.mongo_db.collection(collectionName);
            col.drop();
            util.log("Collection", collectionName, "dropped.");
        });
        */
    } else {
        util.log("Collection", collectionName, " NOT DROPPED");
    }


    var recursivelyInsert = function() {
        var dataPart = data.splice(0, 1000);
        util.log("Insert data", dataPart.length+inserted , " of ", dataStartingLength);
            //var col = self.mongo_db.collection(collectionName);
            col.insert(dataPart, function(err, resultInsert) {
                //util.log('log;', 'Mongo.init test insert, error:', err );
                if (!resultInsert) {
                    throw new Error('test insert failed!');
                }
                inserted+=1000;
                if(inserted < dataStartingLength) setTimeout(recursivelyInsert, 50);
                else self.startNextTask();
            });
    };

    setTimeout(recursivelyInsert, 3000);
};

Migrator.prototype.gameOverTest = function() {
    var self = this;

    util.log("Testing gameOver function started...");

    var col = self.mongo_db.collection('games');
    col.find({mode: "gomoku", action: "game_over"}, {limit:50000}).toArray(function(err, items) {

        var myCounter = 0,
            originalCounter = 0;
        var timeStart = Date.now();

        for(var i=0; i < items.length; i++) {
            var game = items[i];

            game.history = '[' + game.history + ']';
            game.history = game.history.replace(new RegExp('@', 'g'), ',');
            game.history = JSON.parse(game.history);

            var myGameOver = isGameOver(game.history);
            var originalGameOver = game.history[game.history.length - 1].result == 1;

            if(myGameOver != originalGameOver) {
                util.log("DIFFERENT RESULTS game id: ", game._id);
            }

            if(myGameOver) myCounter++;
            if(originalGameOver) originalCounter++;
        }

        util.log("My win counter: ", myCounter, ", original counter: ", originalCounter, " / different: ", Math.abs(originalCounter-myCounter), "\nTime elapsed: ", Date.now() - timeStart);


        self.startNextTask();
    });


/*
    var i = 0;


    util.log("counter", i);*/


};

/*
 * PENALTY
 * */

Migrator.prototype.migratePenalty = function () {
    var self = this;
    util.log("Penalty migration started...");

    this.mysql_con.query('SELECT userid FROM `gomoku_elo_rating_cleanup` WHERE date like "2015-04-01%";',
        function(err, rows, fields) {
            if (err) {
                throw err;
            }
            self.penaltyToMongo(rows);
    });
};

Migrator.prototype.penaltyToMongo = function(penalty_user_ids) {
    var self = this;

    var penalties = [];

    for(var i=0; i < penalty_user_ids.length; i++) {
        var user = penalty_user_ids[i];
        penalties.push({
            "userId" : user.userid.toString(),
            "mode" : "gomoku",
            "type" : 1,
            "time" : 1427846400000,
            "timeAdd" : 1427846400000,
            "value" : -100
        });
    }

    util.log("Penalties length ", penalties.length);
    this.mongoInsert(penalties, 'penalties');
};

/*
* R A T I N G
* */


Migrator.prototype.migrateBRating = function() {
    var self = this;

    util.log("BattleRating migration started...");

    this.mysql_con.query('SELECT br.*, ku.userName, br.win as win, br.lose as lose, UNIX_TIMESTAMP(ku.regTime) as reg \
    from battleship_rating br \
    JOIN kosynka_users ku on ku.userId = br.userid \
    order by id desc \
    limit '+this.conf.limit.rating+';', function(err, rows, fields) { //limit '+this.conf.limit.rating+';
        if (err) {
            throw err;
        }
        self.BattleRatingToMongo(rows);
    });
};

Migrator.prototype.BattleRatingToMongo = function(lg_user) {
    var self = this;

    var parsedUsers = [];

    for(var i=0; i < lg_user.length; i++) {
        var user = lg_user[i];
        parsedUsers.push({
            "userId": ""+user.userid, "userName": user.userName,
            "dateCreate": user.reg*1000, // UNIX time s to ms approximation
            "battleship": {
                "win" : user.win,
                "lose" : user.lose,
                "draw" : 0,
                "minWinHits": user.minshots == 100 ? 1000 : user.minshots,
                "games" : user.win + user.lose,
                "rank" : user.rank,
                "ratingElo" : user.elo,
                "timeLastGame" : Date.now()
            }
        });
    }

    util.log("BattleshipRating length ", parsedUsers.length);
    this.mongoInsert(parsedUsers, 'users');
};

Migrator.prototype.migrateRating = function() {
    var self = this;

    util.log("Rating migration started...");

    this.mysql_con.query('SELECT elo.*, ku.userName, gr.win_online as win, gr.lose_online as lose,  gr.draw_online as draw, UNIX_TIMESTAMP(ku.regTime) as reg \
    from gomoku_elo_rating elo \
    JOIN kosynka_users ku on ku.userId = elo.userid \
    JOIN gomoku_rating gr on gr.userid = elo.userid \
    order by rating_elo desc \
    limit '+this.conf.limit.rating+';', function(err, rows, fields) { //limit '+this.conf.limit.rating+';
        if (err) {
            throw err;
        }
        self.ratingToMongo(rows);
    });
};

Migrator.prototype.ratingToMongo = function(lg_user) {
    var self = this;

    var parsedUsers = [];

    for(var i=0; i < lg_user.length; i++) {
        var user = lg_user[i];
        parsedUsers.push({
            "userId": ""+user.userid, "userName": user.userName,
            "dateCreate": user.reg*1000, // UNIX time s to ms approximation
            "gomoku": {
                "win" : user.win, "lose" : user.lose, "draw" : user.draw,
                "games" : user.win + user.lose + user.draw,
                "rank" : 0, "ratingElo" : user.rating_elo, "timeLastGame" : Date.now()
            }
        });
    }

    util.log("Rating length ", parsedUsers.length);
    this.mongoInsert(parsedUsers, 'users');
};

/*
* C H A T
* */
Migrator.prototype.migrateChat = function() {
    var self = this;

    util.log("Chat migration started...");

    this.mysql_con.query('SELECT chat.*, IFNULL(br.rank, 0) as rank FROM game_chat chat \
    LEFT JOIN   battleship_rating br on chat.userid = br.userid \
    WHERE gamevarid = 18 and visible = 1 order by msgid desc limit '+this.conf.limit.chat+' ', function(err, rows, fields) { //and msgid > 2031186
        if (err) {
            throw err;
        }
        self.chatToMongo(rows);
    });
};

Migrator.prototype.chatToMongo = function(lg_data) {
    var self = this;
    var parsed = [];
    for(var i in lg_data) {
        var data = lg_data[i];
        parsed.push({
            "userId": data.userid.toString(),
            "userName": data.username,
            "admin" : null,
            "time": data.updated_at*1, // UNIX time s to ms approximation
            "target" : data.recipientid == 0 ? "battleship" : data.recipientid.toString(),
            "text": data.msgtext,
            "userData" : {"battleship": {"rank": data.rank}}
        });
    }
    util.log("Chat length: ", parsed.length);
    this.mongoInsert(parsed, 'messages');
};

/*
 * H I S T O R Y
 * */
Migrator.prototype.migrateBHistory = function() {
    var self = this;

    util.log("BattleHistory migration started...");

    this.mysql_con.query('select ku1.userName as owner_userName, ku2.userName as guest_userName, \
    br1.rank as owner_rank, br2.rank as guest_rank, \
    UNIX_TIMESTAMP(bh.date) as timeStart, \
    UNIX_TIMESTAMP(bh.date)as timeEnd, \
    bh.* from battleship_history bh \
    join kosynka_users ku1 on owner_id = ku1.userId \
    join kosynka_users ku2 on guest_id = ku2.userId \
    join battleship_rating br1 on owner_id = br1.userid \
    join battleship_rating br2 on guest_id = br2.userid \
    order by id desc limit '+this.conf.limit.history+'; ', function(err, rows, fields) {
        if (err) {
            throw err;
        }
        self.bHistoryToMongo(rows);
    });
};

Migrator.prototype.bHistoryToMongo = function(lg_data) {
    var self = this;
    var parsed = [];


    util.log("Processing history data, length: ", lg_data.length);

    for(var i=0; i < lg_data.length; i++) {
        var data = lg_data[i];
        var winner = (data.owner_result == 1) ? data.owner_id : data.guest_id;
        var userData = '{\"'+data.owner_id+'\":{\"userId\":\"'+data.owner_id+'\",\"userName\":\"'+data.owner_userName+'\",\"battleship\":{\"rank\":'+data.owner_rank+',\"ratingElo\":'+data.owner_elo+'}},\"'+data.guest_id+'\":{\"userId\":\"'+data.guest_id+'\",\"userName\":\"'+data.guest_userName+'\",\"battleship\":{\"rank\":'+data.guest_rank+', \"ratingElo\":'+data.guest_elo+'}}}';

        parsed.push({
            "timeStart": data.timeStart*1000,
            "timeEnd": data.timeEnd*1000,
            "players": [
                data.owner_id.toString(),
                data.guest_id.toString()
            ],
            "mode": "battleship",
            "winner": winner.toString(),
            "action": "game_over",
            "userData": userData
        });

        if(i%100000 == 0) {
            util.log("current iteration: ", i);
        }
    }
    util.log("Result History length: ", parsed.length);
    this.mongoInsert(parsed, 'history');
};

 Migrator.prototype.migrateHistory = function() {
    var self = this;

    util.log("History migration started...");

    // and gs.level = "human"\

    this.mysql_con.query('select gr.saveid, gs.userid as userid1, gr.oppUserid as userid2, \
        ku.userName as username1, gr.oppUsername as username2,\
        gs.moveorder as player1order,\
        UNIX_TIMESTAMP(gs.created_at) as timeStart,\
        UNIX_TIMESTAMP(gs.updated_at)as timeEnd,\
        gs.result as result1,\
        gs.result_detail as result1detail,\
        IFNULL(elod.elo_rating_dynamic, 1600) as elo1,\
        IFNULL(grating.user_rank, 0) as rank1\
        from gomoku_rooms gr\
        join gomoku_save gs on gs.saveid = gr.saveid\
        join kosynka_users ku on ku.userId = gs.userid\
        join  gomoku_elo_rating_dynamic elod on gs.saveid = elod.saveid\
        join  gomoku_rating grating on gs.userid = grating.userid\
        where gs.gamemode = 0\
        AND gr.saveid < 5787793 \
        AND gr.oppUserid != 0 \
        and gs.board = 19\
        order by saveid desc limit '+this.conf.limit.history+'; ', function(err, rows, fields) {
        if (err) {
            throw err;
        }
        self.historyToMongo(rows);
    });
};

Migrator.prototype.historyToMongo = function(lg_data) {
    var self = this;
    var parsed = [];
    var mergedData = [];

    util.log("Processing history data (step 1 of 2), length: ", lg_data.length);

    for(var i=0; i < lg_data.length; i++) {
        if(lg_data[i-1] && lg_data[i].userid1 == lg_data[i-1].userid2) {
            lg_data[i].elo2 = lg_data[i-1].elo1;
            lg_data[i].rank2 = lg_data[i-1].rank1;
            //lg_data.splice(i-1, 1);

            mergedData.push(lg_data[i]);

            if(i%100000 == 0) {
                util.log("current iteration: ", i);
            }
        }
    }

    util.log("Processing history data (step 2 of 2), length: ", mergedData.length);

    for(var i=0; i < mergedData.length; i++) {
        var data = mergedData[i];
        var winner = (data.result1 == 1) ? data.userid1 : data.userid2;
        var userData = '{\"'+data.userid1+'\":{\"userId\":\"'+data.userid1+'\",\"userName\":\"'+data.username1+'\",\"gomoku\":{\"rank\":'+data.rank1+',\"ratingElo\":'+data.elo1+'}},\"'+data.userid2+'\":{\"userId\":\"'+data.userid2+'\",\"userName\":\"'+data.username2+'\",\"gomoku\":{\"rank\":'+data.rank2+', \"ratingElo\":'+data.elo2+'}}}';

        parsed.push({
            "timeStart": data.timeStart*1000,
            "timeEnd": data.timeEnd*1000,
            "players": [
              data.userid1.toString(),
              data.userid2.toString()
            ],
            "mode": "gomoku",
            "winner": winner.toString(),
            "action": "game_over",
            "userData": userData
        });

        if(i%100000 == 0) {
            util.log("current iteration: ", i);
        }
    }
    util.log("Result History length: ", parsed.length);
    this.mongoInsert(parsed, 'history');
};

new Migrator(conf);

function isGameOver(allTurns) {
    if(allTurns.length == 0) return false;

    var winningLength = 5;
    var turns = [];
    var board = [];

    var board = [];
    for (var i = 0; i < 19; i++) {
        board[i] = [];
        for (var j = 0; j < 19; j++) {
            board[i][j] = null;
        }
    }

    for(var i=0; i < allTurns.length; i++) {
        if(allTurns[i].type == "turn") {
            turns.push(allTurns[i]);
            board[allTurns[i].x][allTurns[i].y] = allTurns[i].color;
        }
    }

    var lastMove = turns[turns.length - 1];


    var directions = ["N", "S", "E", "W", "NW", "NE", "SE", "SW"];
    var counter = {"N": 0, "S": 0, "E": 0, "W": 0, "NW": 0, "NE": 0, "SE": 0, "SW":0};
    var multiplier = {"N": {"row":-1, "col":0}, "S": {"row":1, "col":0},
        "E": {"row":0, "col":1}, "W": {"row":0, "col":-1},
        "NW": {"row":-1, "col":-1}, "NE": {"row":-1, "col":1},
        "SE": {"row":1, "col":1}, "SW": {"row":1, "col":-1} };

    for (var directionIndex in directions) {
        var direction = directions[directionIndex];
        for (var offset = 1; offset < winningLength; offset++) {
            row = lastMove.x + multiplier[direction]["row"] * offset;
            col = lastMove.y + multiplier[direction]["col"] * offset;

            if (board[row]===undefined || board[row][col]===undefined)
                break;

            if (board[row][col] != lastMove.color)
                break;

            counter[direction]++;
        }
    }

    var NorthSouth = 1 + counter["N"] + counter["S"];
    var WestEast = 1 + counter["W"] + counter["E"];
    var NorthWestSouthEast = 1 + counter["NW"] + counter["SE"];
    var NorthEastSouthWest = 1 + counter["NE"] + counter["SW"];

    return (NorthSouth == winningLength) || (WestEast == winningLength) ||
        (NorthWestSouthEast == winningLength) || (NorthEastSouthWest == winningLength);
};