import { initializeApp } from 'firebase/app';
import { getFirestore } from 'firebase/firestore';

const firebaseConfig = {
  apiKey: "AIzaSyCebpTydztL6cKSjDKFOP8LVWI6CCYH9g0",
  authDomain: "pncp-monitor-b3c5f.firebaseapp.com",
  projectId: "pncp-monitor-b3c5f",
  storageBucket: "pncp-monitor-b3c5f.firebasestorage.app",
  messagingSenderId: "995357658004",
  appId: "1:995307658004:web:6e1d404318dc251dbf9d81"
};

const app = initializeApp(firebaseConfig);
export const db = getFirestore(app);
