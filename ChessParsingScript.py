import io
import sqlite3
import random
import chess.pgn
import chess.engine
import zstandard as zstd

def stream_dataset(zstfile, stockfish_path, keep_chance=1.0, threads=1, db_path="Lichess_Comebacks.db", batch_size=5000, time_limit=0.05, seed=50):
    random.seed(seed)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA synchronous = OFF;")
    cursor.execute("PRAGMA journal_mode = MEMORY;") 
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS position_analysis(
            game_id INTEGER PRIMARY KEY AUTOINCREMENT,
            white_elo INTEGER,
            black_elo INTEGER,
            winner TEXT,
            worst_eval_cp INTEGER,
            worst_fen TEXT
        );
    """)
    conn.commit()
    
    engine = chess.engine.SimpleEngine.popen_uci(stockfish_path)
    engine.configure({"Threads": threads})
    engine.configure({"Hash": 512})
    
    dctx = zstd.ZstdDecompressor()
    games_saved = 0
    games_read = 0
    batch = []

    with open(zstfile, 'rb') as compressed_file:
        with dctx.stream_reader(compressed_file) as reader:
            text_stream = io.TextIOWrapper(reader, encoding="utf-8")
            while True:
                if random.random() < keep_chance:
                    game = chess.pgn.read_game(text_stream)
                    if game is None:
                        break
                    
                    result = game.headers.get("Result")
                    games_read += 1
                    if games_read % 50 == 0:
                        print(f"Read {games_read} games, saved {games_saved} so far...")


                    if result == "1-0":
                        winner_color = chess.WHITE
                        winner_str = "White"
                    elif result == "0-1":
                        winner_color = chess.BLACK
                        winner_str = "Black"
                    else:
                        continue
    
                    # get elos for comparison in analysis
                    try:
                        white_elo = int(game.headers.get("WhiteElo", 0))
                        black_elo = int(game.headers.get("BlackElo", 0))
                    except ValueError:
                        continue
                    
                    board = game.board()
                    worst_score = float('inf')
                    worst_fen = ""
                    
                    #Loops through chess moves to find the worst score the winner of the game was at
                    for move in game.mainline_moves():
                        board.push(move)
    
                        try:
                            info = engine.analyse(board, chess.engine.Limit(time=time_limit))
                            current_score = info["score"].pov(winner_color).score(mate_score=10000)
                            if current_score is not None and current_score < worst_score:
                                worst_score = current_score
                                worst_fen = board.fen()
                        except Exception:
                            continue

                    if worst_score != float('inf'):
                        batch.append((white_elo, black_elo, winner_str, worst_score, worst_fen))
                        games_saved += 1
                        
                    # Unified table name to position_analysis here
                    if len(batch) >= batch_size:
                        cursor.executemany("""
                            INSERT INTO position_analysis 
                            (white_elo, black_elo, winner, worst_eval_cp, worst_fen) 
                            VALUES (?, ?, ?, ?, ?)
                        """, batch)
                        conn.commit()
                        batch.clear()
                        print(f"Saved {games_saved} valid decisive games...")
                else:
                    skipped = chess.pgn.skip_game(text_stream)
                    if not skipped: 
                        break
                        
    # Unified table name here as well for the final flush
    if batch:
        cursor.executemany("""
            INSERT INTO position_analysis 
            (white_elo, black_elo, winner, worst_eval_cp, worst_fen) 
            VALUES (?, ?, ?, ?, ?)
        """, batch)
        conn.commit()
        
    engine.quit()
    conn.close()
    print(f"Extraction complete! Saved comeback data for {games_saved} games.")