const express = require('express');
const http = require('http');
const path = require('path');
const { Server } = require('socket.io');
const { WebcastPushConnection } = require('tiktok-live-connector');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

let tiktokConnection = null;
let currentUsername = null;
let isConnected = false;

let stats = {
  gifts: 0,
  likes: 0,
  follows: 0,
  shares: 0,
  comments: 0,
  subs: 0,
  viewers: 0
};

// آخر المشاهدين اللي دخلوا (تيك توك ما يعطينا قائمة كل من يشاهد،
// بس يعطينا إشعار كل ما شخص جديد ينضم للبث)
let recentJoiners = [];

function broadcastStatus() {
  io.emit('status', {
    connected: isConnected,
    username: currentUsername
  });
}

function resetStats() {
  stats = { gifts: 0, likes: 0, follows: 0, shares: 0, comments: 0, subs: 0, viewers: 0 };
  recentJoiners = [];
}

function connectToTikTok(username) {
  if (tiktokConnection) {
    try { tiktokConnection.disconnect(); } catch (e) { /* ignore */ }
  }

  resetStats();
  currentUsername = username;
  isConnected = false;
  tiktokConnection = new WebcastPushConnection(username);

  tiktokConnection.connect()
    .then((state) => {
      isConnected = true;
      console.log(`✅ متصل ببث "${username}" — roomId: ${state.roomId}`);
      broadcastStatus();
    })
    .catch((err) => {
      isConnected = false;
      console.error(`❌ ما قدرنا نتصل ببث "${username}":`, err.message);
      io.emit('status', { connected: false, username, error: err.message });
    });

  tiktokConnection.on('chat', (data) => {
    stats.comments++;
    io.emit('event', {
      type: 'comment',
      user: data.uniqueId,
      nickname: data.nickname || data.uniqueId,
      avatar: data.profilePictureUrl,
      text: data.comment,
      time: Date.now()
    });
    io.emit('stats', stats);
  });

  tiktokConnection.on('gift', (data) => {
    // نتجاهل الحلقات الوسيطة للهدايا المتكررة ونعرضها فقط عند اكتمالها
    const streaking = data.giftType === 1 && !data.repeatEnd;
    if (!streaking) {
      stats.gifts += data.repeatCount || 1;
      io.emit('event', {
        type: 'gift',
        user: data.uniqueId,
        nickname: data.nickname || data.uniqueId,
        avatar: data.profilePictureUrl,
        text: `أرسل هدية "${data.giftName}" ×${data.repeatCount || 1}`,
        image: data.giftPictureUrl,
        time: Date.now()
      });
      io.emit('stats', stats);
    }
  });

  tiktokConnection.on('follow', (data) => {
    stats.follows++;
    io.emit('event', {
      type: 'follow',
      user: data.uniqueId,
      nickname: data.nickname || data.uniqueId,
      avatar: data.profilePictureUrl,
      text: 'تابع الحساب',
      time: Date.now()
    });
    io.emit('stats', stats);
  });

  tiktokConnection.on('share', (data) => {
    stats.shares++;
    io.emit('event', {
      type: 'share',
      user: data.uniqueId,
      nickname: data.nickname || data.uniqueId,
      avatar: data.profilePictureUrl,
      text: 'شارك البث',
      time: Date.now()
    });
    io.emit('stats', stats);
  });

  tiktokConnection.on('like', (data) => {
    const count = data.likeCount || 1;
    stats.likes += count;
    io.emit('event', {
      type: 'like',
      user: data.uniqueId,
      nickname: data.nickname || data.uniqueId,
      avatar: data.profilePictureUrl,
      text: `أرسل ${count} إعجاب`,
      time: Date.now()
    });
    io.emit('stats', stats);
  });

  tiktokConnection.on('subscribe', (data) => {
    stats.subs++;
    io.emit('event', {
      type: 'sub',
      user: data.uniqueId,
      nickname: data.nickname || data.uniqueId,
      avatar: data.profilePictureUrl,
      text: 'اشترك بالحساب',
      time: Date.now()
    });
    io.emit('stats', stats);
  });

  tiktokConnection.on('member', (data) => {
    const joiner = {
      user: data.uniqueId,
      nickname: data.nickname || data.uniqueId,
      avatar: data.profilePictureUrl,
      time: Date.now()
    };
    recentJoiners.unshift(joiner);
    recentJoiners = recentJoiners.slice(0, 50);
    io.emit('viewerJoin', joiner);
  });

  tiktokConnection.on('roomUser', (data) => {
    if (typeof data.viewerCount === 'number') {
      stats.viewers = data.viewerCount;
      io.emit('stats', stats);
    }
  });

  tiktokConnection.on('streamEnd', () => {
    isConnected = false;
    broadcastStatus();
  });

  tiktokConnection.on('disconnected', () => {
    isConnected = false;
    broadcastStatus();
  });
}

io.on('connection', (socket) => {
  socket.emit('stats', stats);
  socket.emit('status', { connected: isConnected, username: currentUsername });
  socket.emit('joinersSnapshot', recentJoiners);
});

app.get('/api/connect', (req, res) => {
  const username = (req.query.uid || '').trim().replace(/^@/, '');
  if (!username) {
    return res.status(400).json({ ok: false, error: 'لازم تحدد اسم المستخدم عبر uid' });
  }
  if (username === currentUsername && isConnected) {
    return res.json({ ok: true, alreadyConnected: true, username });
  }
  connectToTikTok(username);
  res.json({ ok: true, username });
});

app.get('/api/status', (req, res) => {
  res.json({ connected: isConnected, username: currentUsername, stats });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 السيرفر شغال على http://localhost:${PORT}`);
});
