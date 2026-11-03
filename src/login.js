(() => {
  const form = document.getElementById("login-form");
  const input = document.getElementById("password");
  const submit = document.getElementById("submit");
  const error = document.getElementById("login-error");

  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    error.textContent = "";
    submit.disabled = true;
    submit.textContent = "Signing in…";

    let password = input.value;
    const body = JSON.stringify({ password });
    input.value = "";
    password = "";

    try {
      const response = await fetch("/auth/login", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body,
        credentials: "same-origin",
      });
      if (response.ok) {
        window.location.replace("/");
        return;
      }
      if (response.status === 401) {
        error.textContent = "Incorrect password.";
      } else if (response.status === 429) {
        error.textContent = "Too many attempts. Wait one minute and try again.";
      } else {
        error.textContent = "Sign-in is temporarily unavailable.";
      }
    } catch (_) {
      error.textContent = "Sign-in is temporarily unavailable.";
    }

    submit.disabled = false;
    submit.textContent = "Sign in";
    input.focus();
  });
})();
