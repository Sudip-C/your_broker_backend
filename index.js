const express = require('express');
const mysql = require('mysql2');
const cors = require('cors');
const bodyParser = require('body-parser');
require("dotenv").config()

const app = express();

app.use(cors());
app.use(bodyParser.json());

const db = mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: 'Su4ip@123',
    database: 'order_matching'
});

db.connect(err => {
    if (err) throw err;
    console.log('MySQL Connected...');
});

// Get all pending orders
app.get('/api/pending_orders', (req, res) => {
    db.query('SELECT * FROM pending_orders', (err, results) => {
        if (err) throw err;
        res.json(results);
    });
});

// Get all completed orders
app.get('/api/completed_orders', (req, res) => {
    db.query('SELECT * FROM completed_orders', (err, results) => {
        if (err) throw err;
        res.json(results);
    });
});

// Add new order and match orders
app.post('/api/orders', (req, res) => {
    const { buyerQty, buyerPrice, sellerPrice, sellerQty, isBuyer } = req.body;
    if (isBuyer) {
        db.beginTransaction(err => {
            if (err) throw err;

            db.query('SELECT * FROM pending_orders WHERE seller_price <= ?', [buyerPrice], (err, results) => {
                if (err) {
                    return db.rollback(() => {
                        throw err;
                    });
                }

                let qty = buyerQty;

                results.forEach(order => {
                    if (qty === 0) return;

                    const matchedQty = Math.min(qty, order.seller_qty);

                    db.query('INSERT INTO completed_orders (price, qty) VALUES (?, ?)', [order.seller_price, matchedQty], (err) => {
                        if (err) {
                            return db.rollback(() => {
                                throw err;
                            });
                        }
                    });

                    if (matchedQty < order.seller_qty) {
                        db.query('UPDATE pending_orders SET seller_qty = seller_qty - ? WHERE id = ?', [matchedQty, order.id], (err) => {
                            if (err) {
                                return db.rollback(() => {
                                    throw err;
                                });
                            }
                        });
                    } else {
                        db.query('DELETE FROM pending_orders WHERE id = ?', [order.id], (err) => {
                            if (err) {
                                return db.rollback(() => {
                                    throw err;
                                });
                            }
                        });
                    }

                    qty -= matchedQty;
                });

                if (qty > 0) {
                    db.query('INSERT INTO pending_orders (buyer_qty, buyer_price) VALUES (?, ?)', [qty, buyerPrice], (err) => {
                        if (err) {
                            return db.rollback(() => {
                                throw err;
                            });
                        }

                        db.commit(err => {
                            if (err) {
                                return db.rollback(() => {
                                    throw err;
                                });
                            }

                            res.send('Order matched and pending order updated');
                        });
                    });
                } else {
                    db.commit(err => {
                        if (err) {
                            return db.rollback(() => {
                                throw err;
                            });
                        }

                        res.send('Order fully matched');
                    });
                }
            });
        });
    } else {
        db.beginTransaction(err => {
            if (err) throw err;

            db.query('SELECT * FROM pending_orders WHERE buyer_price >= ?', [sellerPrice], (err, results) => {
                if (err) {
                    return db.rollback(() => {
                        throw err;
                    });
                }

                let qty = sellerQty;

                results.forEach(order => {
                    if (qty === 0) return;

                    const matchedQty = Math.min(qty, order.buyer_qty);

                    db.query('INSERT INTO completed_orders (price, qty) VALUES (?, ?)', [order.buyer_price, matchedQty], (err) => {
                        if (err) {
                            return db.rollback(() => {
                                throw err;
                            });
                        }
                    });

                    if (matchedQty < order.buyer_qty) {
                        db.query('UPDATE pending_orders SET buyer_qty = buyer_qty - ? WHERE id = ?', [matchedQty, order.id], (err) => {
                            if (err) {
                                return db.rollback(() => {
                                    throw err;
                                });
                            }
                        });
                    } else {
                        db.query('DELETE FROM pending_orders WHERE id = ?', [order.id], (err) => {
                            if (err) {
                                return db.rollback(() => {
                                    throw err;
                                });
                            }
                        });
                    }

                    qty -= matchedQty;
                });

                if (qty > 0) {
                    db.query('INSERT INTO pending_orders (seller_qty, seller_price) VALUES (?, ?)', [qty, sellerPrice], (err) => {
                        if (err) {
                            return db.rollback(() => {
                                throw err;
                            });
                        }

                        db.commit(err => {
                            if (err) {
                                return db.rollback(() => {
                                    throw err;
                                });
                            }

                            res.send('Order matched and pending order updated');
                        });
                    });
                } else {
                    db.commit(err => {
                        if (err) {
                            return db.rollback(() => {
                                throw err;
                            });
                        }

                        res.send('Order fully matched');
                    });
                }
            });
        });
    }
});
const PORT =process.env.PORT|| 3310
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`)

});
