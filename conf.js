module.exports = {
    tasks: ['b_history',  'b_rating', 'chat'], // 'rating', 'chat', 'history', 'penalty', 'game_over_test', 'b_history',  'b_rating', 'chat'

    mysql: {
        host     : 'localhost',
        user     : 'webadmin',
        password : 'W@bRjltH_game$erV',
        database : 'logicgame'
    },
    mongo: {
        host: '192.168.250.40',
        port: '27001',
        database: '' //battleship, gomoku
    },
    limit: {
        "chat": 10000000,
        "history": 50000000,
        "rating": 100000
    },
    dropCollection: {
        "users": true,
        "history": true,
        "messages": true
    }
};