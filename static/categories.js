[...document.querySelectorAll('form.category:not(.add-category)')]
	.forEach((f) => {
		console.log(f);
		f.addEventListener('submit', (e) => handleForm(f, e))
	});

async function handleForm(f, e) {
	e.preventDefault();
	const formData = new FormData(f);
	const searchParams = new URLSearchParams(formData);
	try {
		f.classList.remove('sent');
		f.classList.remove('error');
		const response = await fetch(f.action, {
			method: f.method,
			body: searchParams,
		});
		if (response.status == 200) {
			f.classList.add('sent');
			setTimeout(() => {
				f.classList.remove('sent');
			}, 2000);
		} else {
			f.classList.add('error');
			setTimeout(() => {
				f.classList.remove('error');
			}, 2000);
			console.error(response);
		}
	} catch(error) {
		f.classList.add('error');
		setTimeout(() => {
			f.classList.remove('error');
		}, 2000);
		console.error(error);
	}
}
