import os
from flask import Flask, render_template, request, session, redirect, url_for
from flask_login import LoginManager, UserMixin, login_required, login_user, \
    current_user, logout_user, fresh_login_required
from flask_sqlalchemy import SQLAlchemy
from flask_session import Session
from urllib.parse import urlparse, urljoin
from psycopg2 import *


# Initiate app
login_manager = LoginManager()
db = SQLAlchemy()


def is_safe_url(target):
    ref_url = urlparse(request.host_url)
    test_url = urlparse(urljoin(request.host_url, target))

    return test_url.scheme in ("http", "https") and ref_url.netloc == \
        test_url.netloc


class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(100), unique=True)
    password = db.Column(db.String(100))
    email = db.Column(db.String(100), unique=True)
    created_at = db.Column(db.DateTime)


class Models(db.Model):
    id = db.Column(db.String, primary_key=True)
    model_id = db.Column(db.String(100))
    url = db.Column(db.String(300))
    maker = db.Column(db.String(100))
    maker_id = db.Column(db.String(100))
    year = db.Column(db.Integer)
    model = db.Column(db.String(100))
    family = db.Column(db.String(100))
    category = db.Column(db.String(100))
    sub_category = db.Column(db.String(100))
    build_kind = db.Column(db.String(100))
    is_frameset = db.Column(db.Boolean)
    is_ebike = db.Column(db.Boolean)
    gender = db.Column(db.String(100))

class Analysis(db.Model):
    id = db.Column(db.String, primary_key=True)
    model_id = db.Column(db.String(100))
    spec_Level_value = db.Column(db.Float)
    group_key = db.Column(db.String(100))
    frame = db.Column(db.Float)
    wheels = db.Column(db.Float)
    brakes = db.Column(db.Float)
    group_set = db.Column(db.Float)
    shifting = db.Column(db.Float)
    seat_post = db.Column(db.Float)
    fork_material = db.Column(db.Float)
    fork_rank = db.Column(db.Float)
    value_prop = db.Column(db.Float)


class Price_history(db.Model):
    id = db.Column(db.String, primary_key=True)
    model_id = db.Column(db.String(100))
    currency = db.Column(db.String(100))
    date = db.Column(db.DateTime)
    amount = db.Column(db.Float)
    change = db.Column(db.Float)
    discounted_amount = db.Column(db.Float)
    discount = db.Column(db.Float)


class Prices(db.Model):
    id = db.Column(db.String, primary_key=True)
    model_id = db.Column(db.String(100))
    currency = db.Column(db.String(100))
    amount = db.Column(db.Float)
    discounted_amount = db.Column(db.Float)
    discount = db.Column(db.Float)


def create_app():
    app = Flask(__name__)
    app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY")

    u = os.environ.get('DB_USERNAME')
    p = os.environ.get('DB_PASSWORD')

    app.config["SQLALCHEMY_DATABASE_URI"] = f"postgresql://" \
                                            f"{u}:{p}@localhost:5432/postgres"
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    login_manager.init_app(app)
    db.init_app(app)

    login_manager.login_view = 'login'
    login_manager.login_message = 'Please log in first'
    login_manager.refresh_view = 'login'
    login_manager.needs_refresh_message = 'You need to log in again'

    @login_manager.user_loader
    def load_user(user_id):
        return User.query.get(int(user_id))

    @app.route("/profile")
    @login_required
    def profile():
        return f"<h1>You are in the profile, {current_user.username}</h1>"

    @app.route("/login", methods=["GET", "POST"])
    def login():
        if request.method == "POST":
            username = request.form.get("username")
            remember_me = bool(request.form.get("remember_me"))
            user = User.query.filter_by(username=username).first()

            if not user:
                return f"<h1>User does not exist</h1>"

            login_user(user, remember=remember_me)

            if "next" in session and session["next"]:
                if is_safe_url(session["next"]):
                    return redirect(session["next"])

            return redirect(url_for("index"))

        session["next"] = request.args.get("next")
        return render_template("login.html")

    @app.route("/logout")
    @login_required
    def logout():
        logout_user()
        return "<h1>You are now logged out</h1>"

    @app.route("/")
    def index():
        return render_template("index.html")

    @app.route("/change")
    @fresh_login_required
    def change():
        return "<h1>This is for fresh logins only</h1>"

    @app.route("/find")
    def find():
        bikes = Models.query.all()
        return render_template("find.html", bikes=bikes)

    return app